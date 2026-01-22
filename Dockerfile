# A development container

FROM debian:bookworm

ARG ZIG_VER=0.15.2
RUN apt-get update
RUN apt-get install -y curl xz-utils
RUN curl https://ziglang.org/download/0.15.2/zig-aarch64-linux-${ZIG_VER}.tar.xz -o zig-linux.tar.xz && \
    tar xf zig-linux.tar.xz && \
    mv zig-aarch64-linux-${ZIG_VER} /usr/local/zig

ENV PATH="/usr/local/zig:${PATH}"
WORKDIR /app
EXPOSE 7733
CMD ["/bin/bash"]
