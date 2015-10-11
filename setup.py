from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()

setup(name='elastictornado',
      version='0.1',
      description='Async ElasticSearch client via the Tornado framework',
      long_description=readme(),
      author='John Ramey',
      author_email='johnramey@gmail.com',
      license='MIT',
      url='http://github.com/ramhiser/elastictornado',
      download_url='http://github.com/ramhiser/elastictornado',
      packages=find_packages(exclude=["tests", "dist"]),
      install_requires=['pyelasticsearch==1.4', 'tornado==4.2.1'],
      setup_requires=['nose==1.3.7'],
      test_suite='nose.collector'
      )
