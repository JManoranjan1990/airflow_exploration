# Use the official PostgreSQL image as the base image
FROM postgres:14.0

# Set custom environment variables (optional)
ENV POSTGRES_USER=Mano
ENV POSTGRES_PASSWORD=Mano
ENV POSTGRES_DB=transaction_records

# Optionally, copy custom initialization scripts (e.g., for creating tables, etc.)
# COPY ./init.sql /docker-entrypoint-initdb.d/

# Expose the PostgreSQL default port
EXPOSE 5432
