# To build a binary using this docker file run, for example:
# docker build -f docker/bin_build.Dockerfile  --build-arg IMAGE_TAG=18_1_71_0 --build-arg TARGET=mr3 .
ARG IMAGE_TAG=18_1_71_0

FROM romange/boost-dev:${IMAGE_TAG} as third_party
ARG IMAGE_TAG

# To allow caching third_party libs via "minimal" build.
# We do not use ARG here, not even reference it here because otherwise it would cause cache misses
# during this STEP for different ARGs.
COPY ./CMakeLists.txt /src/
COPY ./cmake /src/cmake
WORKDIR /build
RUN echo "PATH IS: $PATH" && cmake --version
RUN echo "IMAGE_TAG ${IMAGE_TAG}" && cmake -L -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++ \
                                    -DONLY_THIRD_PARTY=ON -GNinja /src

RUN ninja -j4 protobuf_project glog_project sparsehash_project gperf_project zstd_project \
    lz4_project xxhash_project gtest_project pmr_project

FROM third_party as src
COPY ./ /src/
RUN mkdir /pkg && cmake -L -DONLY_THIRD_PARTY=OFF -DBUILD_DOCS=OFF -GNinja /src

FROM src as bin
# Now really building the target. Previous containers will be reused between the builds.
ARG TARGET
ARG IMAGE_TAG
RUN echo "Building ${TARGET}" && ninja -j4 $TARGET && strip $TARGET && mv $TARGET /pkg/
RUN ldd -r /pkg/$TARGET | grep "/build/third_party" | awk '{print $3}' | \
    while read n; do cp -nH $n /pkg/; done

FROM romange/boost-prod:${IMAGE_TAG}
ARG TARGET
COPY --from=bin /pkg/ /app/
RUN ldconfig /app && ln -s $TARGET start_app
ENV PATH /app:$PATH

ENTRYPOINT ["start_app"]
