from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="assets_dbt_python",
        packages=find_packages(exclude=["assets_dbt_python_tests"]),
        package_data={"assets_dbt_python": ["dbt_project/*"]},
        install_requires=[
            "dagster",
            "dagster-dbt",
            "dagster-snowflake",
            "dagster-snowflake-pandas",
            "pandas",
            "numpy",
            "scipy",
            "dbt-core",
            "dbt-snowflake",
        ],
        extras_require={"dev": ["dagit", "pytest"]},
    )
