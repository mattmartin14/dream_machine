import requests as rq

def test():
	print('123')
	z = "test vim stuff"
	print(z)
	x = "blah"
	z = "test vim stuff"
	print(f'value of x is {x}')
	x = rq.get('https://www.google.com')
	print(x.Response())	
	for i in range(5):
		print(i+1)	
    
    
	print(1)

test()

