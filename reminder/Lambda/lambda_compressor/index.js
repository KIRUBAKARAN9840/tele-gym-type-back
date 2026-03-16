// index.js
// Node.js 20 + aws-sdk v2 + sharp
// EventBridge(S3) -> SQS -> Lambda: compress images to ~150 KB,
// overwrite same key, and tag to avoid loops.

const AWS = require("aws-sdk");          // bundle aws-sdk@2
const sharp = require("sharp");

// ---------- Tunables ----------
const TARGET = 150 * 1024;               // ~150 KB target
const MIN_WIDTH = 640;                   // don't shrink below this width
const DOWNSCALE_STEP = 0.85;             // downscale 15% each pass
const MIN_QUALITY = 50;                  // quality floor
const USE_WEBP = true;                   // prefer WebP for non-alpha
const MAX_PASSES = 6;                    // stop after N encode passes
const WEBP_EFFORT = 4;                   // 0-6 (6 slowest, best); 4 is fast/good

// ---------- AWS SDK (v2) client with sane retries/timeouts ----------
const s3 = new AWS.S3({
  maxRetries: 3,
  httpOptions: { timeout: 30000, connectTimeout: 5000 },
  retryDelayOptions: { base: 300 }
});

// ---------- helpers ----------
const isImageKey = (key) => /\.(jpe?g|png|webp|tiff?)$/i.test(key);

const printMem = (label) => {
  const mu = process.memoryUsage();
  const mb = (b) => (b / 1024 / 1024).toFixed(2);
  console.log(`🧠 ${label} | rss=${mb(mu.rss)}MB heapUsed=${mb(mu.heapUsed)}MB ext=${mb(mu.external)}MB`);
};

// Accept both raw S3 event and EventBridge S3 event (via SQS)
function extractS3EventsFromAny(body) {
  // A) Raw S3 event (S3 -> SQS)
  if (Array.isArray(body?.Records) && body.Records[0]?.s3) {
    return body.Records.map(r => ({
      bucket: r.s3.bucket.name,
      key: r.s3.object.key
    }));
  }
  // B) EventBridge S3 event (S3 -> EventBridge -> SQS target)
  if (body?.source === "aws.s3" && body?.detail?.bucket?.name && body?.detail?.object?.key) {
    return [{
      bucket: body.detail.bucket.name,
      key: body.detail.object.key
    }];
  }
  return [];
}

// S3 op with retry/backoff
async function retryS3(operation, name, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (err) {
      const isLast = attempt === maxRetries;
      const retryable = err.retryable !== false && (err.statusCode >= 500 || err.code === "SlowDown");
      console.log(`[${name}] Attempt ${attempt}/${maxRetries} failed: ${err.code || err.message}`);
      if (isLast || !retryable) {
        console.error(`[${name}] Failed after ${attempt} attempts:`, err);
        throw err;
      }
      const delay = 500 * Math.pow(2, attempt - 1);
      console.log(`[${name}] Retrying in ${delay}ms…`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

async function markDone(Bucket, Key) {
  await s3.putObjectTagging({
    Bucket, Key, Tagging: { TagSet: [{ Key: "compressed", Value: "true" }] }
  }).promise();
}

function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks)));
  });
}

// The core encoder with progress prints every pass.
async function compressToTarget(inputBuf, hasAlpha) {
  const started = Date.now();
  let width = null;
  let quality = 80;
  let out = inputBuf;
  let contentType = null;

  for (let pass = 1; pass <= MAX_PASSES; pass++) {
    const meta = await sharp(inputBuf).metadata();
    if (!width) width = meta.width;

    if (hasAlpha) {
      out = await sharp(inputBuf)
        .resize({ width })
        .png({ compressionLevel: 9, adaptiveFiltering: true })
        .toBuffer();
      contentType = "image/png";
    } else if (USE_WEBP) {
      out = await sharp(inputBuf)
        .resize({ width })
        .webp({ quality, effort: WEBP_EFFORT })
        .toBuffer();
      contentType = "image/webp";
    } else {
      out = await sharp(inputBuf)
        .resize({ width })
        .jpeg({ quality, progressive: true, mozjpeg: true })
        .toBuffer();
      contentType = "image/jpeg";
    }

    console.log(
      `🌀 pass ${pass}: width=${width}, q=${quality}, size=${(out.length / 1024 / 1024).toFixed(2)} MB`
    );

    if (out.length <= TARGET) break;

    if (!hasAlpha && quality > MIN_QUALITY) {
      quality -= 10;
      continue;
    }
    if (width > MIN_WIDTH) {
      width = Math.floor(width * DOWNSCALE_STEP);
      continue;
    }

    // no more room to reduce without breaching floors
    break;
  }

  console.log(`⏳ encode elapsed ${(Date.now() - started)} ms`);
  return { buffer: out, contentType };
}

// ---------- Lambda handler ----------
exports.lambdaHandler = async (event) => {
  const records = event.Records ?? [];

  for (const rec of records) {
    // body may be an SQS message (stringified JSON) or undefined during tests
    const body = rec.body ? JSON.parse(rec.body) : {};
    const items = extractS3EventsFromAny(body);
    if (items.length === 0) continue;

    for (const { bucket, key } of items) {
      const Bucket = bucket;
      const Key = decodeURIComponent(String(key).replace(/\+/g, " "));
      console.log(`\n🔍 Processing: s3://${Bucket}/${Key}`);

      try {
        if (!isImageKey(Key)) {
          console.log(`⏭️  Non-image, tagging and skipping`);
          await markDone(Bucket, Key);
          continue;
        }

        // skip if already compressed
        const tags = await s3.getObjectTagging({ Bucket, Key }).promise().catch(() => ({ TagSet: [] }));
        const already = tags.TagSet?.some(t => t.Key === "compressed" && t.Value === "true");
        if (already) {
          console.log(`⏭️  Already compressed`);
          continue;
        }

        // HEAD: for original size, metadata
        const head = await retryS3(
          () => s3.headObject({ Bucket, Key }).promise(),
          `headObject(${Key})`
        );

        const origBytes = head.ContentLength ?? 0;
        const origMB = (origBytes / 1024 / 1024).toFixed(2);
        console.log(`📥 Downloading ${Key} (orig=${origMB} MB)`);
        printMem("start");

        // Small already? tag and skip
        if (origBytes <= TARGET) {
          console.log(`⏭️  Already small (${(origBytes / 1024).toFixed(1)} KB)`);
          await markDone(Bucket, Key);
          continue;
        }

        // GET: download body
        const obj = await retryS3(
          () => s3.getObject({ Bucket, Key }).promise(),
          `getObject(${Key})`
        );
        const bodyBuf = Buffer.isBuffer(obj.Body) ? obj.Body : await streamToBuffer(obj.Body);
        console.log(`📦 Buffer in-memory size=${(bodyBuf.length / 1024 / 1024).toFixed(2)} MB`);
        printMem("after download");

        // Probe alpha channel
        const meta = await sharp(bodyBuf).metadata();
        const hasAlpha = !!meta.hasAlpha;

        // Compress
        console.log(`🔧 Compressing ${Key}…`);
        const encoded = await compressToTarget(bodyBuf, hasAlpha);

        const newMB = (encoded.buffer.length / 1024 / 1024).toFixed(2);
        const savings = ((1 - encoded.buffer.length / origBytes) * 100).toFixed(1);
        console.log(`✅ ${Key}: ${origMB} MB → ${newMB} MB (${savings}% saved)`);

        // Preserve existing metadata + flag
        const mergedMeta = { ...(head.Metadata || {}), compressed: "true" };

        // PUT: overwrite
        await retryS3(
          () => s3.putObject({
            Bucket,
            Key,
            Body: encoded.buffer,
            ContentType: encoded.contentType,
            Metadata: mergedMeta
          }).promise(),
          `putObject(${Key})`
        );

        // Also add object tag to avoid reprocessing
        await markDone(Bucket, Key);

        printMem("done");

      } catch (err) {
        console.error(`❌ Error processing ${key}:`, err?.message || err);
        // If the object disappeared, tag to avoid infinite retries
        if (err?.statusCode === 404 || err?.code === "NoSuchKey") {
          await markDone(bucket, key).catch(() => {});
        }
      }
    }
  }

  return { ok: true };
};

// also export "handler" for index.handler
exports.handler = exports.lambdaHandler;
