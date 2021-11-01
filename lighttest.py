#measuring the light intensity using a photocell
import RPi.GPIO as GPIO,time,os               #import the libraries
DEBUG=1
GPIO.setmode(GPIO.BOARD)
GPIO.setwarnings(False)
def RCtime(RCpin):   # function start
	reading=0
	GPIO.setup(RCpin,GPIO.OUT)
	GPIO.output(RCpin,GPIO.LOW)
	time.sleep(2)                                    # time to discharge capacitor
	GPIO.setup(RCpin,GPIO.IN)
	while (GPIO.input(RCpin) == GPIO.LOW): 
# the loop will run till the capacitor  is charged
		reading += 1                                
# measuring time which in turn is measuring resistance
	return reading                               
# function
while True:
	print RCtime(12)      # calling the function