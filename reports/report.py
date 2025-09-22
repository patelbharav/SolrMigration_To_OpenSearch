from jinja2 import FileSystemLoader

from config import get_custom_logger
import jinja2
from jinja2 import Environment, select_autoescape, FileSystemLoader
logger = get_custom_logger("reports.report")


class Report:
    def __init__(self):

        self.field_types_solr = 0
        self.field_types_os = 0
        self.field_types_error = 0

        self.field_solr = 0
        self.field_os = 0
        self.field_error = 0

        self.dynamic_field_solr = 0
        self.dynamic_field_os = 0
        self.dynamic_field_error = 0

        self.copy_field_solr = 0
        self.copy_field_os = 0
        self.copy_field_error = 0
        
        # Data migration metrics
        self.data_migration_enabled = False
        self.data_migration_docs_total = 0
        self.data_migration_docs_exported = 0
        self.data_migration_batches = 0
        self.data_migration_errors = 0

        self.field_type_exception_list = []
        self.field_exception_list = []
        self.dynamic_field_exception_list = []
        self.copy_field_exception_list = []
        self.data_migration_error_list = []
        
    def add_data_migration_error(self, error_msg):
        """Add data migration error to the report"""
        self.data_migration_errors += 1
        self.data_migration_error_list.append(str(error_msg))
        
    def update_data_migration_stats(self, enabled=False, total=0, exported=0, batches=0):
        """Update data migration statistics"""
        self.data_migration_enabled = enabled
        self.data_migration_docs_total = total
        self.data_migration_docs_exported = exported
        self.data_migration_batches = batches

    def __print_summary(self):
        report = "Summary Reports " + "\n"
        report = report + "===========================================================" + "\n"
        report = report + "Solr Field types found: {}".format(self.field_types_solr) + "\n"
        report = report + "Solr Field types mapped: {}".format(self.field_types_os) + "\n"
        report = report + "Solr Field types error: {}".format(self.field_types_error) + "\n"
        report = report + "===========================================================" + "\n"
        report = report + "Solr Field  found: {}".format(self.field_solr) + "\n"
        report = report + "Solr Field mapped: {}".format(self.field_os) + "\n"
        report = report + "Solr Field  error: {}".format(self.field_error) + "\n"
        report = report + "===========================================================" + "\n"
        report = report + "Solr Dynamic Field  found: {}".format(self.dynamic_field_solr) + "\n"
        report = report + "Solr Dynamic Field mapped: {}".format(self.dynamic_field_os) + "\n"
        report = report + "Solr Dynamic Field  error: {}".format(self.dynamic_field_error) + "\n"
        report = report + "===========================================================" + "\n"
        report = report + "Solr Copy Field  found: {}".format(self.copy_field_solr) + "\n"
        report = report + "Solr Copy Field mapped: {}".format(self.copy_field_os) + "\n"
        report = report + "Solr Copy Field  error: {}".format(self.copy_field_error) + "\n"
        report = report + "===========================================================" + "\n"
        
        if self.data_migration_enabled:
            report = report + "Data Migration Enabled: Yes" + "\n"
            report = report + "Total Documents: {}".format(self.data_migration_docs_total) + "\n"
            report = report + "Documents Exported: {}".format(self.data_migration_docs_exported) + "\n"
            report = report + "Batches Processed: {}".format(self.data_migration_batches) + "\n"
            report = report + "Data Migration Errors: {}".format(self.data_migration_errors) + "\n"
            report = report + "===========================================================" + "\n"
            
        return report

    def report(self, file):
        environment = jinja2.Environment(loader=FileSystemLoader("reports/templates/"), autoescape=True)
        template = environment.get_template("results.html")

        summary = {
            'field_types': {
                "total": self.field_types_solr,
                "mapped": self.field_types_os,
                "error": self.field_types_error,
                "exception_list": self.field_type_exception_list
            },
            'fields': {
                "total": self.field_solr,
                "mapped": self.field_os,
                "error": self.field_error,
                "exception_list": self.field_exception_list
            },
            'dynamic_fields': {
                "total": self.dynamic_field_solr,
                "mapped": self.dynamic_field_os,
                "error": self.dynamic_field_error,
                "exception_list": self.dynamic_field_exception_list

            },
            'copy_fields': {
                "total": self.copy_field_solr,
                "mapped": self.copy_field_os,
                "error": self.copy_field_error,
                "exception_list": self.copy_field_exception_list
            }
        }

        context = {
            "summary": summary
        }
        content = template.render(context)
        
        # Create directory if it doesn't exist
        import os
        os.makedirs(os.path.dirname(file) if os.path.dirname(file) else '.', exist_ok=True)
        
        with open(file, mode="w", encoding="utf-8") as message:
            message.write(content)
            
    def data_migration_report(self, file):
        """Generate a separate report for data migration"""
        environment = jinja2.Environment(loader=FileSystemLoader("reports/templates/"), autoescape=True)
        template = environment.get_template("data_migration_report.html")
        
        data_migration = {
            "enabled": self.data_migration_enabled,
            "total": self.data_migration_docs_total,
            "exported": self.data_migration_docs_exported,
            "batches": self.data_migration_batches,
            "errors": self.data_migration_errors,
            "error_list": self.data_migration_error_list
        }
        
        context = {
            "data_migration": data_migration
        }
        content = template.render(context)
        
        # Create directory if it doesn't exist
        import os
        os.makedirs(os.path.dirname(file) if os.path.dirname(file) else '.', exist_ok=True)
        
        with open(file, mode="w", encoding="utf-8") as message:
            message.write(content)




