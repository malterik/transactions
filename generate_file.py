NUMBER_OF_ENTRIES = 3000000

if __name__ == "__main__":
    f = open("data/huge.csv", "w")
    f.writelines("type,client,tx,amount\n")
    for i in range(0, NUMBER_OF_ENTRIES):
        f.writelines(f"deposit,{i % 2^16},{i},1.0\n")
        if i % 1000 == 0:
            print(f"{i}")
