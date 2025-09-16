def print_args(*args):
 print("Positional arguments received:", args)
# Example with **kwargs
def print_kwargs(**kwargs):
 print("Keyword arguments received:", kwargs)
print_args(1, 2, 3) # Outputs: Positional arguments received: (1, 2, 3)
print_kwargs(name="Alice", age=25)