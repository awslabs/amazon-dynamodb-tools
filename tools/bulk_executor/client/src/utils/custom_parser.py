import sys
from argparse import ArgumentParser, RawDescriptionHelpFormatter

class BulkArgumentParser(ArgumentParser):
    def __init__(self, *args, help_text=None, **kwargs):
        # Set formatter_class to RawDescriptionHelpFormatter to preserve text formatting
        kwargs['formatter_class'] = kwargs.get('formatter_class', RawDescriptionHelpFormatter)

        # If help_text is provided, use it as the description
        if help_text:
            kwargs['description'] = help_text

        super().__init__(*args, **kwargs)
        self.help_text = help_text

    def error(self, message):
        """Custom error handling that includes help text"""
        self.print_help(sys.stderr)
        self.exit(2, f'\nerror: {message}\n')

    def print_help(self, file=None):
        """Override print_help to ensure consistent formatting"""
        super().print_help(file or sys.stdout)

    def format_help(self):
        """Override format_help to customize the help output if needed"""
        return super().format_help()
