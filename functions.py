def factorial(x):
    if x == 1:
        return 1
    else:
        return x * factorial(x - 1)


num = 8
flag = False
print(factorial(num))


def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)


print(fibonacci(num))


if num > 1:
    for i in range(2, num):
        if (num % i) == 0:
            flag = True
            break

if flag:
    print(num, "is not prime")
else:
    print(num, "is prime")


