import RPi.GPIO as GPIO
import time

GPIO.setmode(GPIO.BOARD)
GPIO.setwarnings(False)
GPIO.setup(19, GPIO.IN)
while True:
    print(GPIO.input(19))
    time.sleep(2)


