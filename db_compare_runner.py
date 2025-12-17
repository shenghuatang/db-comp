"""
YAML-based configuration runner for database comparisons
Similar to simple_compare.py but for database sources
"""

import yaml
from yaml.loader import SafeLoader
import argparse
import logging
from db_compare import DBCompare, DataSource, create_data_source_from_dict
import tracemalloc
import time
from typing import Dict, Any


version = "1.0.0"
tracemalloc.start()


def create_log(path: str) -> logging.Logger:
    """
    Creates a rotating log

    Args:
        path: Log file path

    Returns:
        Logger object
    """
    from logging.handlers import RotatingFileHandler

    logger = logging.getLogger("db_compare_runner")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    # File handler
    handler = RotatingFileHandler(path, maxBytes=1024*1024*10, backupCount=5)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.addHandler(ch)

    return logger


def run_comparison_job(job_name: str, job_config: Dict[str, Any], db_connections: Dict[str, Any], log: logging.Logger):
    """
    Run a single comparison job

    Args:
        job_name: Name of the job
        job_config: Job configuration dictionary
        db_connections: Dictionary of reusable database connections
        log: Logger object
    """
    log.info("=" * 80)
    log.info(f"Running job: {job_name}")
    log.info("=" * 80)

    try:
        # Extract configuration
        data1_config = job_config['data_source1'].copy()
        data2_config = job_config['data_source2'].copy()

        # Resolve connection references if present
        if 'connection' in data1_config:
            conn_name = data1_config['connection']
            if conn_name not in db_connections:
                raise ValueError(f"Connection '{conn_name}' not found in db_connections")
            # Merge connection details with data source config
            conn_details = db_connections[conn_name].copy()
            conn_details.update(data1_config)  # data_source config overrides connection config
            data1_config = conn_details
            log.info(f"Using connection '{conn_name}' for data_source1")

        if 'connection' in data2_config:
            conn_name = data2_config['connection']
            if conn_name not in db_connections:
                raise ValueError(f"Connection '{conn_name}' not found in db_connections")
            # Merge connection details with data source config
            conn_details = db_connections[conn_name].copy()
            conn_details.update(data2_config)  # data_source config overrides connection config
            data2_config = conn_details
            log.info(f"Using connection '{conn_name}' for data_source2")

        # Create data sources
        log.info("Creating data source 1...")
        data_source1 = create_data_source_from_dict(data1_config)

        log.info("Creating data source 2...")
        data_source2 = create_data_source_from_dict(data2_config)

        # Get comparison parameters
        join_columns = job_config['join_columns']
        comparing_columns = job_config.get('comparing_columns')
        column_mapping = job_config.get('column_mapping', {})
        tolerance = job_config.get('tolerance', {})
        abs_tol = job_config.get('abs_tol', 0)
        rel_tol = job_config.get('rel_tol', 0)
        show_join_columns_both_sides = job_config.get('show_join_columns_both_sides', False)
        show_transformed_columns = job_config.get('show_transformed_columns', False)
        output_dir = job_config.get('output_dir', f'output/{job_name}')
        log_file = job_config.get('log_file', f'{job_name}_comparison.log')

        # Report generation flags
        generate_full_csv = job_config.get('generate_full_csv', True)
        generate_diff_csv = job_config.get('generate_diff_csv', True)
        generate_summary = job_config.get('generate_summary', True)
        generate_side_by_side_excel = job_config.get('generate_side_by_side_excel', False)
        validate_dups = job_config.get('validate_duplicates', True)

        # Create comparator
        comparator = DBCompare(
            data_source1=data_source1,
            data_source2=data_source2,
            join_columns=join_columns,
            comparing_columns=comparing_columns,
            column_mapping=column_mapping,
            tolerance=tolerance,
            abs_tol=abs_tol,
            rel_tol=rel_tol,
            show_join_columns_both_sides=show_join_columns_both_sides,
            show_transformed_columns=show_transformed_columns,
            output_dir=output_dir,
            log_file=log_file
        )

        # Run comparison
        results = comparator.run_comparison(
            generate_full_csv=generate_full_csv,
            generate_diff_csv=generate_diff_csv,
            generate_summary=generate_summary,
            generate_side_by_side_excel=generate_side_by_side_excel,
            validate_dups=validate_dups
        )

        log.info(f"Job '{job_name}' completed successfully")
        log.info(f"Results: {results}")

        return results

    except Exception as e:
        log.exception(f"Job '{job_name}' failed with error:")
        raise


def main(yaml_file: str = "db_compare.yaml", job: str = "",log: logging.Logger = None):
    """
    Main entry point for YAML-based comparison

    Args:
        yaml_file: Path to YAML configuration file
        job: Specific job name to run (empty = run all jobs)
    """
    # Load YAML configuration
    with open(yaml_file) as f:
        data = yaml.load(f, Loader=SafeLoader)

    comparisons = data.get("comparisons", {})
    db_connections = data.get("db_connections", {})  # Get reusable connections

    if not comparisons:
        log.error("No comparisons defined in YAML file")
        return

    if db_connections:
        log.info(f"Loaded {len(db_connections)} reusable database connection(s)")

    # Track overall results
    total_jobs = 0
    successful_jobs = 0
    failed_jobs = 0

    # Track matching results
    jobs_with_differences = []
    jobs_with_matches = []
    jobs_failed_execution = []
    job_results = {}  # Store detailed results per job

    # Run jobs
    for job_name, job_config in comparisons.items():
        # Check if we should run this job
        should_run = True
        if job:
            should_run = (job_name == job)

        if should_run:
            total_jobs += 1
            try:
                results = run_comparison_job(job_name, job_config, db_connections, log)  # Pass db_connections
                successful_jobs += 1

                # Store results
                job_results[job_name] = results

                # Categorize by matching status
                if results['different_rows'] > 0 or results['only_in_source1'] > 0 or results['only_in_source2'] > 0:
                    jobs_with_differences.append(job_name)
                    log.warning(f"Job '{job_name}' - DIFFERENCES FOUND: {results['different_rows']} different rows, "
                               f"{results['only_in_source1']} only in source1, {results['only_in_source2']} only in source2")
                else:
                    jobs_with_matches.append(job_name)
                    log.info(f"Job '{job_name}' - PERFECT MATCH: All {results['total_rows']} rows match")

            except Exception as e:
                failed_jobs += 1
                jobs_failed_execution.append(job_name)
                log.error(f"Job '{job_name}' failed: {e}")
                # Continue with other jobs
        else:
            log.debug(f"Skipping job: {job_name}")

    # Summary
    log.info("=" * 80)
    log.info("Overall Summary")
    log.info("=" * 80)
    log.info(f"Total jobs: {total_jobs}")
    log.info("")
    log.info("EXECUTION STATUS:")
    log.info(f"  Successfully Executed: {successful_jobs}")
    log.info(f"  Failed Execution: {failed_jobs}")
    if jobs_failed_execution:
        log.error(f"  Jobs that failed: {', '.join(jobs_failed_execution)}")
    log.info("")
    log.info("MATCHING STATUS:")
    log.info(f"  Perfect Matches: {len(jobs_with_matches)}")
    if jobs_with_matches:
        log.info(f"    Jobs: {', '.join(jobs_with_matches)}")
    log.info(f"  With Differences: {len(jobs_with_differences)}")
    if jobs_with_differences:
        log.warning(f"    Jobs: {', '.join(jobs_with_differences)}")

    # Write summary to file
    if total_jobs > 0:
        # Determine output directory
        # If single job, use that job's output directory
        # If multiple jobs, use a common output directory
        if total_jobs == 1 and len(comparisons) > 0:
            first_job_name = list(comparisons.keys())[0]
            output_dir = comparisons[first_job_name].get('output_dir', f'output/{first_job_name}')
        else:
            output_dir = "output"

        # Create output directory if it doesn't exist
        import os
        os.makedirs(output_dir, exist_ok=True)

        summary_file = os.path.join(output_dir, "comparison_summary.txt")
        json_summary_file = os.path.join(output_dir, "comparison_summary.json")

        # Text summary
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("DATABASE COMPARISON SUMMARY\n")
            f.write("="*80 + "\n\n")

            f.write("EXECUTION STATUS:\n")
            f.write(f"  Total Jobs: {total_jobs}\n")
            f.write(f"  Successfully Executed: {successful_jobs}\n")
            f.write(f"  Failed Execution: {failed_jobs}\n")
            if jobs_failed_execution:
                f.write(f"  Failed Jobs: {', '.join(jobs_failed_execution)}\n")
            f.write("\n")

            f.write("MATCHING STATUS:\n")
            f.write(f"  Perfect Matches: {len(jobs_with_matches)}\n")
            if jobs_with_matches:
                f.write(f"    Jobs: {', '.join(jobs_with_matches)}\n")
            f.write(f"  With Differences: {len(jobs_with_differences)}\n")
            if jobs_with_differences:
                f.write(f"    Jobs: {', '.join(jobs_with_differences)}\n")
            f.write("\n")

            f.write("="*80 + "\n")
            f.write("DETAILED RESULTS:\n")
            f.write("="*80 + "\n\n")

            for job_name, results in job_results.items():
                f.write(f"Job: {job_name}\n")
                f.write(f"  Execution Status: SUCCESS\n")
                if results['different_rows'] > 0 or results['only_in_source1'] > 0 or results['only_in_source2'] > 0:
                    f.write(f"  Matching Status: DIFFERENCES FOUND\n")
                else:
                    f.write(f"  Matching Status: PERFECT MATCH\n")
                f.write(f"  Total Rows: {results['total_rows']}\n")
                f.write(f"  Equal Rows: {results['equal_rows']}\n")
                f.write(f"  Different Rows: {results['different_rows']}\n")
                f.write(f"  Only in Source1: {results.get('only_in_source1', 0)}\n")
                f.write(f"  Only in Source2: {results.get('only_in_source2', 0)}\n")
                f.write(f"  Match Percentage: {results['match_percentage']:.2f}%\n")
                f.write("\n")

        log.info(f"Summary written to: {summary_file}")

        # JSON summary
        import json
        from datetime import datetime

        json_summary = {
            "summary_metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_jobs": total_jobs,
                "yaml_file": yaml_file
            },
            "execution_status": {
                "total_jobs": total_jobs,
                "successfully_executed": successful_jobs,
                "failed_execution": failed_jobs,
                "failed_jobs": jobs_failed_execution
            },
            "matching_status": {
                "perfect_matches": len(jobs_with_matches),
                "jobs_with_perfect_match": jobs_with_matches,
                "with_differences": len(jobs_with_differences),
                "jobs_with_differences": jobs_with_differences
            },
            "jobs": []
        }

        # Add detailed results for each job
        for job_name, results in job_results.items():
            has_differences = results['different_rows'] > 0 or results['only_in_source1'] > 0 or results['only_in_source2'] > 0

            job_detail = {
                "job_name": job_name,
                "execution_status": "SUCCESS",
                "matching_status": "DIFFERENCES_FOUND" if has_differences else "PERFECT_MATCH",
                "metrics": {
                    "total_rows": results['total_rows'],
                    "equal_rows": results['equal_rows'],
                    "different_rows": results['different_rows'],
                    "only_in_source1": results.get('only_in_source1', 0),
                    "only_in_source2": results.get('only_in_source2', 0),
                    "in_both": results.get('in_both', 0),
                    "match_percentage": round(results['match_percentage'], 2)
                },
                "output_files": {
                    "output_dir": comparisons[job_name].get('output_dir', f'output/{job_name}'),
                    "reports": []
                }
            }

            # Add report file paths
            output_dir = comparisons[job_name].get('output_dir', f'output/{job_name}')
            if comparisons[job_name].get('generate_full_csv', True):
                job_detail["output_files"]["reports"].append({
                    "type": "full_csv",
                    "path": f"{output_dir}/comparison_report.csv"
                })
            if comparisons[job_name].get('generate_diff_csv', True):
                job_detail["output_files"]["reports"].append({
                    "type": "differences_csv",
                    "path": f"{output_dir}/differences_only.csv"
                })
            if comparisons[job_name].get('generate_summary', True):
                job_detail["output_files"]["reports"].append({
                    "type": "summary",
                    "path": f"{output_dir}/summary_report.txt"
                })
            if comparisons[job_name].get('generate_side_by_side_excel', False):
                job_detail["output_files"]["reports"].append({
                    "type": "excel",
                    "path": f"{output_dir}/side_by_side_comparison.xlsx"
                })

            json_summary["jobs"].append(job_detail)

        # Add failed jobs
        for job_name in jobs_failed_execution:
            job_detail = {
                "job_name": job_name,
                "execution_status": "FAILED",
                "matching_status": "N/A",
                "metrics": None,
                "output_files": None,
                "error": "Job failed during execution"
            }
            json_summary["jobs"].append(job_detail)

        # Write JSON file
        with open(json_summary_file, 'w', encoding='utf-8') as f:
            json.dump(json_summary, f, indent=2, ensure_ascii=False)

        log.info(f"JSON summary written to: {json_summary_file}")

    # Return exit code based on results
    if failed_jobs > 0:
        log.error("Some jobs failed to execute")
        return 1  # Execution failure
    elif len(jobs_with_differences) > 0:
        log.warning("All jobs executed successfully, but differences were found")
        return 2  # Execution success, but data differences found
    else:
        log.info("All jobs executed successfully with perfect matches")
        return 0  # Complete success

def main_run():
    print(f"Database Compare Runner version: {version}")

    parser = argparse.ArgumentParser(
        description="Database comparison using YAML configuration",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-y", "--yaml", help="YAML configuration file", default="db_compare.yaml")
    parser.add_argument("-o", "--output", help="Log output file", default="db_compare_runner.log")
    parser.add_argument("-j", "--job", help="Run specific job defined in YAML", default="")

    args = parser.parse_args()

    yaml_file = args.yaml
    log_path = args.output
    job = args.job

    # Create logger
    log = create_log(log_path)

    log.info(f"Using configuration from {yaml_file}")
    if job:
        log.info(f"Running specific job: {job}")
    else:
        log.info("Running all jobs")

    # Start timer
    start_time = time.time()

    try:
        main(yaml_file, job,log)
    except Exception as e:
        log.exception("Runner failed with error:")

    # End timer
    end_time = time.time()
    elapsed_time = end_time - start_time

    log.info(f"Total elapsed time: {elapsed_time:.2f} seconds")

    # Memory usage
    current, peak = tracemalloc.get_traced_memory()
    log.info(f"Memory usage: current {current/1024/1024:.2f}MB, peak {peak/1024/1024:.2f}MB")

    tracemalloc.stop()


def test_run():
    print(f"Database Compare Runner version: {version}")

    yaml_file = "db_compare.yaml"
    log_path = "db_compare_runner.log"
    job = "customer_comparison"
    # Create logger
    log = create_log(log_path)

    log.info(f"Using configuration from {yaml_file}")
    if job:
        log.info(f"Running specific job: {job}")
    else:
        log.info("Running all jobs")

    # Start timer
    start_time = time.time()

    try:
        main(yaml_file, job,log)
    except Exception as e:
        log.exception("Runner failed with error:")

    # End timer
    end_time = time.time()
    elapsed_time = end_time - start_time

    log.info(f"Total elapsed time: {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    test_run()

