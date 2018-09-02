FROM romange/u18builder as build_third_party

# To allow caching third_party libs via "minimal" build.
# We do not use ARG here, not even reference it here because otherwise it would cause cache misses
# during this STEP for different ARGs.
COPY ./CMakeLists.txt /src/
COPY ./cmake /src/cmake
WORKDIR /tmp/build
RUN cmake -L -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=/usr/bin/g++ -DONLY_THIRD_PARTY=ON -GNinja /src
RUN ninja -j4 protobuf_project glog_project sparsehash_project gperf_project

FROM build_third_party
# Now we use ARGs.
ARG TARGET

# Now really building the target. Previous containers will be reused between the builds.
COPY ./ /src/
RUN cmake -L -DONLY_THIRD_PARTY=OFF -GNinja /src
RUN ninja -j4 $TARGET
