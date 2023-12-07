echo "copying common libs to working directory....."
cp ../common_lib/*.py .
echo "starting docker build"
docker build -t lmpeiris/sma-ui:latest .
echo "Running container"
docker run -d --env-file ../env.list -p 8150:8150 --name=sma-ui lmpeiris/sma-ui
