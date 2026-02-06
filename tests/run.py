# Python
import pytest, sys, os

if __name__ == '__main__':
    root_dir = os.getcwd()
    sys.path.append(root_dir)

    coverage = ["j_perm"]

    extra_args = sys.argv[1:]
    if len(extra_args) == 0:
        extra_args = ["tests"]

    reports_dir = os.path.join("tests", "report", "results")
    coverage_dir = os.path.join("tests", "report", "coverage")
    os.makedirs(reports_dir, exist_ok=True)
    os.makedirs(coverage_dir, exist_ok=True)
    html_report_path = os.path.join(reports_dir, "test_report.html")
    coverage_report_path = os.path.join(reports_dir, "test_coverage.html")

    args = [("--cov=" + x) for x in coverage] \
           + [f"--cov-report=html:{coverage_dir}", "--cov-config=tests/.coveragerc"] \
           + [f"--html={html_report_path}", "--self-contained-html"] \
           + extra_args

    pytest.main(args)