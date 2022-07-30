import os


class Config:
    DATA_DIR = os.path.expanduser('~/.fin-models-data')

    DATABASE_URI = '{engine}://{user}:{pw}@{host}:{port}/{db}'.format(
        engine=os.getenv('SQLALCHEMY_DATABASE_ENGINE', 'postgresql+psycopg2'),
        user=os.getenv('SQLALCHEMY_DATABASE_USER', 'fun_techan'),
        pw=os.getenv('SQLALCHEMY_DATABASE_PASSWORD', 'fun_techan'),
        host=os.getenv('SQLALCHEMY_DATABASE_HOST', '127.0.0.1'),
        port=os.getenv('SQLALCHEMY_DATABASE_PORT', 5432),
        db=os.getenv('SQLALCHEMY_DATABASE_NAME', 'fun_techan'),
    )

    POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', 'RphS7ZhWO4ZD_uV8WF8w9dZiZbe63Qd6')
