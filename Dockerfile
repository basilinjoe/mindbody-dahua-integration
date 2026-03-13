FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency manifest + lockfile first (layer cache)
COPY pyproject.toml uv.lock ./

# Install prod deps only into .venv
RUN uv sync --frozen --no-dev

# Add venv binaries to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy application code
COPY . .
