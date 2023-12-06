from multiprocessing import Process

def f(name):
    print('hello', name)
    
name_list = ['bob', 'jane', 'tom']

if __name__ == '__main__':
    p = Process(target=f, args=(name_list, ))
    p.start()
    p.join()
