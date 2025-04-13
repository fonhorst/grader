from abc import ABC, abstractmethod
from typing import List, Optional
from pydantic import BaseModel


class CheckReport(BaseModel):
    """Represents a result of an individual check"""
    required: bool
    passed: bool
    check_description: str
    reason: Optional[str] = None
    check_group: Optional[str] = None


class CheckerReport(BaseModel):
    """Structured log of checking"""
    checks: List[CheckReport] = []
    fail_reason: Optional[str] = None

    def has_success(self) -> bool:
        """Check if all checks are successful"""
        return not self.fail_reason and all(check.passed for check in self.checks if check.required)

    def has_warnings(self) -> bool:
        """Check if any checks are failed"""
        return any(not check.passed for check in self.checks if not check.required)

    def success(self, description: str, required: bool = True, check_group: Optional[str] = None):
        """Add a success check report to the checker report"""
        self.checks.append(CheckReport(required=required, passed=True, check_description=description, check_group=check_group))
        return self

    def fail(self, description: str, reason: str, required: bool = True, check_group: Optional[str] = None):
        """Add an error check report to the checker report"""
        self.checks.append(CheckReport(required=required, passed=False, check_description=description, reason=reason, check_group=check_group))
        return self

    def add(self, other: 'CheckReport'):
        """Add a check report to the checker report"""
        self.checks.append(other)
        return self

    def include(self, other: 'CheckerReport', check_group: Optional[str] = None):
        """Include another checker report into this one"""
        if check_group:
            self.checks.extend((check.copy(update={"check_group": check_group}) for check in other.checks))
        else:
            self.checks.extend(other.checks)
        return self

    def to_markdown(self) -> str:
        """Generate a Markdown report from the checker results

        Returns:
            str: Markdown formatted report
        """
        # Group checks by pass/fail and required/optional
        required_passed = []
        required_failed = []
        optional_passed = []
        optional_failed = []

        for check in self.checks:
            if check.required and check.passed:
                required_passed.append(check)
            elif check.required and not check.passed:
                required_failed.append(check)
            elif not check.required and check.passed:
                optional_passed.append(check)
            else:  # not required and not passed
                optional_failed.append(check)

        # Generate markdown report
        md = []
        md.append("# ClickHouse Lab Checker Report\n")

        # Summary section
        md.append("## Summary\n")
        overall_status = "✅ **PASSED**" if self.has_success() else "❌ **FAILED**"
        md.append(f"**Overall Status:** {overall_status}\n")

        total_checks = len(self.checks)
        required_checks = len([c for c in self.checks if c.required])
        passed_required = len(required_passed)

        md.append(f"**Required Checks:** {passed_required}/{required_checks} passed\n")

        if optional_passed or optional_failed:
            optional_checks = len([c for c in self.checks if not c.required])
            passed_optional = len(optional_passed)
            md.append(f"**Optional Checks:** {passed_optional}/{optional_checks} passed\n")

        # Required failures (most critical information)
        if required_failed:
            md.append("\n## Required Checks That Failed\n")
            for i, check in enumerate(required_failed, 1):
                md.append(f"### {i}. {check.check_description}\n")
                md.append(f"**Reason:** {check.reason}\n")

        # Required successes
        if required_passed:
            md.append("\n## Required Checks That Passed\n")
            for check in required_passed:
                md.append(f"- {check.check_description}\n")

        # Optional failures
        if optional_failed:
            md.append("\n## Optional Checks That Failed\n")
            for i, check in enumerate(optional_failed, 1):
                md.append(f"### {i}. {check.check_description}\n")
                md.append(f"**Reason:** {check.reason}\n")

        # Optional successes
        if optional_passed:
            md.append("\n## Optional Checks That Passed\n")
            for check in optional_passed:
                md.append(f"- {check.check_description}\n")

        # Join all markdown sections with newlines
        return "".join(md)


class CheckableQuery(BaseModel):
    """Represents a query that can be validated"""
    query: str
    description: str

    def validate(self, result) -> bool:
        """Validate the query result"""
        return result is not None and len(result) > 0


class LabChecker(ABC):
    @abstractmethod
    def run_checks(self) -> CheckerReport:
        ...