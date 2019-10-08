# How to build docker image with a gaia binary

I've prepared development and production docker images based on Ubuntu that allow building self
contained GAIA binaries. The build process is as followse:

 1. [bin_build.Dockerfile](../docker/bin_build.Dockerfile) orchestrates the build. It requires few build arguments to pass:
    * the binary target to build
    * optional ubuntu and boost versions
    * image tag name for the newly created image.
 2. The script pulls the development image with specified ubuntu version and boost development    libraries installed (by default it's Ubuntu 18 with Boost 1.71) and builds the specified target. Thus no meddling with host system is needed.
 3. Then the script pulls the appropriate run-time image of the same version but with runtime boost libraries installed and copies there the built binary and its shared lib dependencies.
 Therefore, the final image is as slim as possible and much smaller than the development image.
 4. Finally it tags the image locally with the specified tag name.

The `docker build` command should be run from gaia root dir and it looks as follows:

```bash
> docker build -f docker/bin_build.Dockerfile  --build-arg TARGET=<binary_target> .  -t <image_name>
```

for example,

```bash
> docker build -t asio_fibers:mytag -f docker/bin_build.Dockerfile  --build-arg TARGET=asio_fibers .

> docker run asio_fibers:mytag --logtostderr
```

Optionally, it's possible to pass build argument `IMAGE_TAG=...` . Currently, supported values are: ` 16_1_71_0`, ` 16_1_70_0`, ` 18_1_69_0`, ` 18_1_70_0`, ` 18_1_71_0`.
