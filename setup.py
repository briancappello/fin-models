from setuptools import setup, find_packages


with open('README.md', encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='fin-models',
    version='0.1.0',
    description='Financial Database Models',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/briancappello/fin-models',
    author='Brian Cappello',
    license='MIT',

    packages=find_packages(exclude=['docs', 'tests']),
    include_package_data=True,
    zip_safe=False,
    python_requires='>=3.8',
    install_requires=[
        'aiohttp',
        'beautifulsoup4',
        'click',
        'lxml',
        'pandas',
        'pandas-market-calendars',
        'requests',
        'sqlalchemy-unchained',
    ],
    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    entry_points='''
        [console_scripts]
        fin=fin_models.cli:cli
    ''',
)
