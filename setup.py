from setuptools import find_packages, setup
from typing import List


def get_requirements() -> List[str]:
    """
    This function will return a list of requirements.
    """
    requirement_list: List[str] = []
    
    # Open and read the requirements.txt file
    with open('requirements.txt', 'r') as file:
        requirement_list = file.read().splitlines()
    
    # Remove the '-e .' entry if it exists
    requirement_list = [req for req in requirement_list if req.strip() != '-e .']
    
    return requirement_list


setup(
    name="APS Fault Detection System",
    version="0.0.1",
    author="Bapan Bairagya",
    author_email="bapanmldl7892@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements(),
)
