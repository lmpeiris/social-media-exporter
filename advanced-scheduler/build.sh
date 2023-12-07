echo "copying common libs to working directory....."
cp ../common_lib/*.py .
echo "starting docker build"
docker build -t lmpeiris/sma-advanced-scheduler:latest .
echo "Running container"
docker run -d --env-file ../env.list --name=sma-advanced-scheduler lmpeiris/sma-advanced-scheduler
