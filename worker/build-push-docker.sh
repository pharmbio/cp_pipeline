
if [[ $# -eq 0 ]] ; then
    echo 'You need to specify version as argument, e.g. build-push-docker.sh v4.0.7'
    exit 0
fi

VERSION=$1

docker build -t "ghcr.io/pharmbio/cpp_worker:$VERSION-latest" .
docker push "ghcr.io/pharmbio/cpp_worker:$VERSION-latest"

read -p "Do you want to push with \"stable\" tag also? [y|n]" -n 1 -r < /dev/tty
echo
if ! grep -qE "^[Yy]$" <<< "$REPLY"; then
    exit 1
fi

docker build -t "ghcr.io/pharmbio/cpp_worker:$VERSION-stable" .
docker push "ghcr.io/pharmbio/cpp_worker:$VERSION-stable"
