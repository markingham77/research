import torch
import click
from time import process_time

def testgpu(n1,n2):
    if torch.backends.mps.is_available():
        mps_device = torch.device("mps")
    t0 = process_time()
    x = torch.ones(n1, device=mps_device)
    y = x + torch.rand(n1, device=mps_device)
    t1 = process_time()
    print(f"Total time with gpu ({n1}): {t1-t0}")
    t0 = process_time()
    x = torch.ones(n2, device=mps_device)
    y = x + torch.rand(n2, device=mps_device)
    t1 = process_time()
    print(f"Total time with gpu ({n2}): {t1-t0}")

def testcpu(n1,n2):
    t0 = process_time()
    x = torch.ones(n1)
    y = x + torch.rand(n1)
    t1 = process_time()
    print(f"Total time with cpu ({n1}): {t1-t0}")
    t0 = process_time()
    x = torch.ones(n2)
    y = x + torch.rand(n2)
    t1 = process_time()
    print(f"Total time with cpu ({n2}): {t1-t0}")



@click.command()
# @click.option('--epochs', default=100)
# @click.option('--chunk_size', default=200)
@click.option('--test', is_flag=True, default=True, help='tests if basic torch operation works')
@click.option('--verify_gpu', is_flag=True, default=False, help='check if pytorch installation is set up to use macbook M1 GPU')
@click.option('--speed', is_flag=True, default=False, help='tests speed of CPU versus GPU for small (CPU should be quicker) and large (GPU should be quicker) tensors')
def cli(test,verify_gpu,speed):    
    """
    Simple program that verifies and tests how well your machine can run pytorch (eg, pytorch2. is able to
    utilise modern Macbook GPUs).
    """
    # print('load is',load)
    # print('write is',write)

    if speed:
        testcpu(10000,100000000)
        testgpu(10000,100000000)
        return

    if verify_gpu:
        cli_verify_gpu()
        return
    
    if test:
        cli_test()
        return 
    
def cli_verify_gpu():
    if torch.backends.mps.is_available():
        mps_device = torch.device("mps")
        x = torch.ones(1, device=mps_device)
        print (x)
    else:
        print ("MPS device not found.")

def cli_test():
    x = torch.rand(5, 3)
    print(x)

if __name__=='__main__':
    cli()


