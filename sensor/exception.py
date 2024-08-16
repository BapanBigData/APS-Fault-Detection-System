import sys


def get_error_message_detail(error: Exception, error_detail: sys) -> str:
    """
    Constructs a detailed error message including the script name, line number, and the error message.

    Args:
        error (Exception): The exception object that was raised.
        error_detail (sys): The sys module, used to extract detailed traceback information.

    Returns:
        str: A formatted string containing the script name, line number, and error message.
    """
    _, _, exc_tb = error_detail.exc_info()
    file_name = exc_tb.tb_frame.f_code.co_filename
    error_message = f"Error occurred in script: [{file_name}] at line: [{exc_tb.tb_lineno}] with message: [{str(error)}]"
    
    return error_message



class SensorException(Exception):
    """
    Custom exception class for handling errors.

    Attributes:
        error_message (str): Detailed error message with script name, line number, and the original error message.
    """

    def __init__(self, error_message: Exception, error_detail: sys) -> None:
        """
        Initializes the SensorException with a detailed error message.

        Args:
            error_message (str): The error message associated with the exception.
            error_detail (sys): The sys module used to extract detailed traceback information.
        """
        super().__init__(error_message)
        self.error_message = get_error_message_detail(error_message, error_detail)

    def __str__(self) -> str:
        """
        Returns the string representation of the exception, which is the detailed error message.

        Returns:
            str: The detailed error message.
        """
        return self.error_message
