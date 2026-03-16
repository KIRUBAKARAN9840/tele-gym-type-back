#!/bin/bash
# Deployment script for lambda_compressor (Node.js 20 + Sharp)

set -e

echo "🚀 Building Lambda Compressor deployment package..."
echo ""

# Clean previous builds
echo "🧹 Cleaning previous builds..."
rm -rf node_modules package-lock.json lambda_compressor_deployment.zip

# Install dependencies (sharp needs platform-specific binaries)
echo "📦 Installing dependencies for AWS Lambda (linux-x64)..."

if command -v docker >/dev/null 2>&1; then
  echo "🐳 Docker detected – installing dependencies inside Lambda base image..."
  if ! docker run --rm \
      -v "$(pwd)":/var/task \
      -w /var/task \
      public.ecr.aws/lambda/nodejs:20 \
      /bin/bash -lc "npm install --omit=dev --arch=x64 --platform=linux --include=optional"; then
    echo "⚠️ Docker build failed, falling back to local install. Ensure you're on Linux (x64) or the build may fail."
    npm install --omit=dev --arch=x64 --platform=linux --include=optional
  fi
else
  echo "⚠️ Docker not found, falling back to local install. Ensure you're on Linux (x64) or the build may fail."
  npm install --omit=dev --arch=x64 --platform=linux --include=optional
fi

# Create deployment zip
echo "📦 Creating deployment package..."
zip -r lambda_compressor_deployment.zip index.js node_modules/ -q

# Cleanup
echo "🧹 Cleaning up..."
rm -rf node_modules package-lock.json

FILE_SIZE=$(du -h lambda_compressor_deployment.zip | cut -f1)
echo ""
echo "✅ Deployment package ready: lambda_compressor_deployment.zip ($FILE_SIZE)"
echo ""
echo "📝 Next steps:"
echo "   1. Upload lambda_compressor_deployment.zip to AWS Lambda"
echo "   2. Set runtime to: Node.js 20.x"
echo "   3. Set handler to: index.lambdaHandler (or index.handler)"
echo "   4. Set timeout to: 60 seconds"
echo "   5. Set memory to: 512 MB"
echo "   6. Add S3 read/write permissions to Lambda role"
echo "   7. Connect SQS queue as trigger (you'll provide the SQS URL)"
echo ""
echo "🎯 Lambda is ready for deployment!"
