num = 3
def factorial(x):
    if x == 1:
        return 1
    else:
        return x * factorial(x - 1)

print(factorial(num))

def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)

print(fibonacci(num))

def prime(p):
    flag = False
    if p > 1:
        for i in range(2, p):
            if (p % i) == 0:
                flag = True
                break
    if flag:
        print(p, "is not prime")
    else:
        print(p, "is prime")
    return p

prime(num)

