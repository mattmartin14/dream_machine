import requests as rq

def test():
	print('123')
	x = "blah"
	print(f'value of x is {x}')
	x = rq.get('https://www.google.com')
	print(x.Response())	
	for i in range(5):
		print(i+1)	

test()

