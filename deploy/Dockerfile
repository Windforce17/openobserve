FROM debian:trixie-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl procps && \
    rm -rf /var/lib/apt/lists/* && \
    update-ca-certificates
COPY openobserve /openobserve
COPY GIT_COMMIT /GIT_COMMIT
RUN grep -qa mimalloc /openobserve
RUN /openobserve init-dir -p /data/
ARG GIT_COMMIT=unknown
LABEL git_commit=$GIT_COMMIT
CMD ["/openobserve"]
