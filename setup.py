from setuptools import setup, find_packages
from setuptools.extension import Extension

from Cython.Build import cythonize


extensions = [
]


setup(
    name = 'aiocurl',
    version = '0.1',
    packages = find_packages(),
    include_package_data = True,

    entry_points = {
        'console_scripts': [
        ],
    },

    install_requires = [
    ],

    ext_modules = cythonize(extensions),

    author = 'Gu Pengfei',
    author_email = 'gpfei96@gmail.com',
    description = 'A fast HTTP client for asyncio, based on libcurl',
    license = 'MIT',
)

