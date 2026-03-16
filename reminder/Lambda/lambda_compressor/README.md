# Lambda Image Compressor (Node.js + Sharp)

Non-blocking image compression for Fittbot uploads. Compresses images to ~150KB in background.

## Architecture

```
S3 Upload → S3 Event Notification → Two SQS Queues (parallel)
                                     ├─> fittbot-post-uploads (existing)
                                     │   └─> lambda_feed (marks complete)
                                     │
                                     └─> fittbot-image-compression (NEW)
                                         └─> lambda_compressor (compresses)
```

**Non-blocking:** Posts appear immediately with original images, compression happens in background.

## Build & Deploy

```bash
cd "/Users/apple/Documents/naveen/AWS and Local/reminder/Lambda/lambda_compressor"
chmod +x deploy.sh
./deploy.sh
```

This creates `lambda_compressor_deployment.zip` (~50MB with Sharp library).

## AWS Lambda Setup

1. **Create Lambda Function**
   - Name: `fittbot-image-compressor`
   - Runtime: **Node.js 20.x**
   - Handler: `index.lambdaHandler` (or `index.handler`)
   - Timeout: **60 seconds**
   - Memory: **512 MB**
   - Upload: `lambda_compressor_deployment.zip`

2. **IAM Permissions** (add to Lambda execution role):
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:PutObject",
           "s3:GetObjectTagging",
           "s3:PutObjectTagging"
         ],
         "Resource": "arn:aws:s3:::fittbot-uploads/*"
       },
       {
         "Effect": "Allow",
         "Action": [
           "sqs:ReceiveMessage",
           "sqs:DeleteMessage",
           "sqs:GetQueueAttributes"
         ],
         "Resource": "arn:aws:sqs:ap-south-2:*:fittbot-image-compression"
       }
     ]
   }
   ```

   **Note:** `s3:GetObject` permission covers both GetObject and HeadObject API calls.

3. **Create SQS Queue** (you'll do this):
   - Name: `fittbot-image-compression`
   - Type: Standard
   - Visibility timeout: **90 seconds** (longer than Lambda timeout)
   - Message retention: 4 days
   - Delivery delay: **10 seconds** (let lambda_feed process first)

4. **Connect SQS to Lambda**:
   - Lambda → Add trigger → SQS
   - Select: `fittbot-image-compression`
   - Batch size: **1**

5. **Update S3 Event Notification**:
   - Go to S3 bucket: `fittbot-uploads`
   - Properties → Event notifications
   - **Add new notification** (keep existing one!):
     - Event name: `image-compression-notification`
     - Events: `s3:ObjectCreated:*`
     - Prefix: `post_uploads/`
     - Destination: SQS queue `fittbot-image-compression`

## Supported Formats

**Input formats (all will be compressed):**
- ✅ JPEG / JPG
- ✅ PNG (with transparency support)
- ✅ WebP
- ✅ TIFF / TIF

**Output formats:**
- 📸 **WebP** (default) - Best compression, smaller files, 30-50% better than JPEG
- 📸 **JPEG** (if USE_WEBP = false) - Universal compatibility
- 📸 **PNG** (for images with transparency/alpha channel)

**WebP Browser Support:** ✅ Chrome, ✅ Firefox, ✅ Safari 14+, ✅ Edge (97%+ global support)

## Configuration

Edit constants in `index.js`:
- `TARGET = 150 * 1024` - Target 150KB
- `MIN_WIDTH = 640` - Don't shrink below this
- `MIN_QUALITY = 50` - JPEG/WebP quality floor
- `USE_WEBP = true` - Use WebP output (better compression than JPEG)

## Retry Logic & Fault Tolerance

**Built-in AWS SDK retries:**
- Max retries: 3 attempts
- Timeout: 30 seconds per request
- Connection timeout: 5 seconds
- Exponential backoff: 300ms base delay

**Application-level retries:**
- Critical operations (headObject, getObject, putObject) have additional retry logic
- Exponential backoff: 500ms → 1s → 2s
- Only retries on 5xx errors (server issues)
- Fails fast on 4xx errors (client issues like 404)

**Error handling:**
- Logs all errors with details (error code, status, message)
- Missing files (404) are tagged to skip reprocessing
- Individual file failures don't stop the batch
- CloudWatch logs show retry attempts for debugging

## How It Works

1. User uploads image → S3
2. S3 sends event to **TWO** SQS queues:
   - Queue 1 → `lambda_feed` (marks post complete, broadcasts)
   - Queue 2 → `lambda_compressor` (compresses in background)
3. Compression strategy:
   - Download original image from S3
   - Detect if image has transparency (alpha channel)
   - If **has transparency**: Compress to PNG (keeps transparency)
   - If **no transparency**: Compress to WebP (better compression)
   - Iteratively reduce quality (80 → 70 → 60 → 50)
   - If still too large, reduce dimensions by 15% per iteration
   - Target: ~150KB, minimum width: 640px
   - Overwrite same S3 key with compressed version
   - Tag as `compressed=true` to avoid reprocessing loops
4. Users see original first, then compressed version loads automatically

## Testing

After deployment:
```bash
cd "/Users/apple/Documents/naveen/AWS and Local"
python app/load_test_posts_v3_presigned.py
```

Monitor CloudWatch Logs for both Lambdas.

## Troubleshooting

**Lambda timeout**: Increase memory to 1024 MB (more memory = faster CPU)

**Still reprocessing images**: Check S3 tags have `compressed=true`

**Images not compressing**: Check CloudWatch logs, verify SQS queue has messages

**Sharp errors**: Ensure deployed with `--platform=linux` flag (deploy.sh handles this)
