"""
Setup configuration for Goodreads Recommendation System

This setup.py file configures the Python package for the Goodreads recommendation
system, enabling installation and distribution of the data pipeline modules.

Package includes:
- Data cleaning and preprocessing modules
- Feature engineering components
- Data validation and anomaly detection
- Normalization and staging table promotion
- Comprehensive test suite

Author: Goodreads Recommendation Team
Date: 2025
"""

from setuptools import find_packages, setup

setup(
    name="goodreads_recommendations",  # Package name
    version="0.1",                     # Package version
    packages=find_packages(),          # Automatically find all packages
)