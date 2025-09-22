FROM opensearchproject/opensearch-dashboards:2.19
# Remove security plugin and its configurations
RUN /usr/share/opensearch-dashboards/bin/opensearch-dashboards-plugin remove securityDashboards || true
# Remove any existing opensearch_dashboards.yml
RUN rm -f /usr/share/opensearch-dashboards/config/opensearch_dashboards.yml
# Add our configuration
COPY --chown=opensearch-dashboards:opensearch-dashboards  opensearch_dashboards.yml /usr/share/opensearch-dashboards/config/

# Run as non-root user
USER opensearch-dashboards

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 CMD curl -f http://localhost:5601/api/status || exit 1
