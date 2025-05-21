# py-api-catalog
# RPortela API Catalog

A modern API catalog system for managing and discovering datasets.

## Features

- 📦 Comprehensive dataset management
- 🔍 Advanced search and discovery
- 🌐 RESTful API endpoints
- 🔐 Secure authentication and authorization
- 📊 Prometheus metrics and monitoring
- 📚 Extensive documentation
- 🔄 Real-time updates with WebSocket
- 📈 Vector search integration

## Installation

1. Clone the repository:
```bash
git clone https://github.com/rportela/py-api-catalog.git
cd py-api-catalog
```

2. Install dependencies:
```bash
poetry install
```

3. Copy and modify the environment file:
```bash
cp .env.example .env
```

4. Run migrations:
```bash
poetry run alembic upgrade head
```

5. Start the development server:
```bash
poetry run uvicorn app.main:app --reload
```

## Configuration

The application uses environment variables for configuration. Create a `.env` file with the following variables:

```env
# Database Configuration
DATABASE_URL=postgresql://user:password@localhost:5432/catalog_db

# JWT Configuration
SECRET_KEY=your-secret-key
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30

# Server Configuration
HOST=0.0.0.0
PORT=8000
ENVIRONMENT=development

# ChromaDB Configuration
CHROMA_PERSIST_DIRECTORY=./chroma_db
CHROMA_COLLECTION_NAME=dataset_embeddings
```

## API Documentation

The API is documented using OpenAPI/Swagger. Once the server is running, you can access the documentation at:

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Testing

Run tests with coverage:
```bash
poetry run pytest --cov=src
```

## Development

The project uses pre-commit hooks for code formatting and linting. Before committing, run:
```bash
pre-commit run --all-files
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache-2.0 License - see the LICENSE file for details.

## Support

For support, please open an issue in the GitHub repository.
