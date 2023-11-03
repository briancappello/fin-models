#!/bin/bash

poetry run isort . && poetry run black .
