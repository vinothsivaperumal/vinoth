from mod import CustomException

try:
    raise CustomException('text')
except CustomException as error:
    print(error.value)
