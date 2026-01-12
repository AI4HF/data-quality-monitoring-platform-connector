# Build
docker build -t srdc/data-quality-monitoring-platform-connector:latest .

# Login
docker login nexus.srdc.com.tr:18445

# Tag & Push
docker tag srdc/data-quality-monitoring-platform-connector:latest nexus.srdc.com.tr:18445/srdc/data-quality-monitoring-platform-connector:latest
docker push nexus.srdc.com.tr:18445/srdc/data-quality-monitoring-platform-connector:latest
