from sqlalchemy_unchained import *

from .config import Config


engine, Session, Model, relationship = init_sqlalchemy_unchained(Config.DATABASE_URI)
