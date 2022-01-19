import sys

if __name__ == "__main__":
    f = open("data/huge.csv", "w")
    f.writelines("type,client,tx,amount\n")
    for i in range(0, int(sys.argv[1])):
        f.writelines(f"deposit,{i % 2^16},{i},1.0\n")
