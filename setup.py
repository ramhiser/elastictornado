from setuptools import setup, find_packages

with open("README.md") as readme:
    long_description = readme.read()
    with open("README", "w") as pypi_readme:
        pypi_readme.write(long_description)

setup(name='elastictornado',
      version='0.1',
      description='Async ElasticSearch client via the Tornado framework',
      long_description=long_description,
      author='John Ramey',
      author_email='johnramey@gmail.com',
      license='MIT',
      url='http://github.com/ramhiser/elastictornado',
      download_url='http://github.com/ramhiser/elastictornado',
      packages=find_packages(exclude=["tests", "dist"]),
      install_requires=['pyelasticsearch==1.4', 'tornado==4.2.1'],
      setup_requires=['nose==1.3.7']
      )
