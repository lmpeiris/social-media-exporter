echo "copying common libs to working directory....."
cp ../common_lib/*.py .
echo "starting docker build"
docker build -t lmpeiris/youtube-adapter:latest .
echo "Running container"
docker run -d --env-file ./env.list -p 9130:9130 --name=social-media-exporter lmpeiris/youtube-adapter
