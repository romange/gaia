FROM romange/cpp-dev18 as third_party

# To allow caching third_party libs via "minimal" build.
# We do not use ARG here, not even reference it here because otherwise it would cause cache misses
# during this STEP for different ARGs.
COPY ./CMakeLists.txt /src/
COPY ./cmake /src/cmake
WORKDIR /build
RUN cmake -L -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=/usr/bin/g++ -DONLY_THIRD_PARTY=ON -GNinja /src
RUN ninja -j4 protobuf_project glog_project sparsehash_project gperf_project zstd_project \
    evhtp_project lz4_project xxhash_project

FROM third_party as src
COPY ./ /src/
RUN mkdir /pkg && cmake -L -DONLY_THIRD_PARTY=OFF -GNinja /src

FROM src as bin
# Now really building the target. Previous containers will be reused between the builds.
ARG TARGET

RUN echo "Building ${TARGET}" && ninja -j4 $TARGET && strip $TARGET && mv $TARGET /pkg/
RUN ldd -r /pkg/$TARGET | grep "/build/third_party" | awk '{print $3}' | \
    while read n; do cp -nH $n /pkg/; done

FROM romange/u18prod
ARG TARGET
COPY --from=bin /pkg/ /app/
RUN ldconfig /app && ln -s $TARGET start_app
ENV PATH /app:$PATH

ENTRYPOINT ["start_app"]
