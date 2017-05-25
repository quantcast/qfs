rm -Rf .bin
mkdir .bin
docker build -t qfs-build .
docker run --rm -v $PWD/.bin:/code/qfs/build/release/bin qfs-build

