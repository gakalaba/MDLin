files = ["2024-12-07-08-07-27/2024-12-07-08-08-55/2024-12-07-08-08-55/out/client-0-0/client-0-0-0-stderr-0.log", "2024-12-07-14-57-39/2024-12-07-14-59-12/2024-12-07-14-59-12/out/client-0-0/client-0-0-0-stderr-0.log"]
def takeavg(L):
    r = 0
    for l in L:
        r += l
    return r/len(L)


def takemedian(L):
    L = sorted(L)
    return L[len(L)//2]
'''
for f in files:
    with open(f) as my_file:
        writebyte = []
        marshall = []
        flush = []
        # WriteByte=210ns, Marshal=541ns, Flush=5.4µs
        for line in my_file:
            if "WriteByte" in line:
                lines = line.split(",")
                writebyte.append(float(lines[0].split("=")[1][:-2]))
                marshall.append(float(lines[1].split("=")[1][:-2]))
                flush.append(float(lines[2].split("=")[1][:-3]))
        
        print(f"Avg writeByte = {takeavg(writebyte)} and median writeByte = {takemedian(writebyte)}")
        print(f"Avg marshall = {takeavg(marshall)} and median marshall = {takemedian(marshall)}")
        print(f"Avg flush = {takeavg(flush)} and median flush = {takemedian(flush)}")
'''

files = ["2024-12-07-17-50-37/2024-12-07-17-52-06/2024-12-07-17-52-07/out/client-0-0/client-0-0-0-stderr-0.log", "2024-12-07-17-46-11/2024-12-07-17-47-45/2024-12-07-17-47-45/out/client-0-0/client-0-0-0-stderr-0.log"]
'''
for f in files:
    with open(f) as my_file:
        appreq = []
        # app req took 13.33809ms
        for line in my_file:
            if "app req took" in line:
                lines = line.split(" ")
                if lines[-1][-3:-1] == "ms":
                    appreq.append(float(lines[-1][:-3]) * 1000)
                else:
                    appreq.append(float(lines[-1][:-3]))
        
        print(f"Avg appreq = {takeavg(appreq)} and median appreq = {takemedian(appreq)}")
'''

files = ["2024-12-07-18-11-15/2024-12-07-18-12-44/2024-12-07-18-12-45/out/client-0-0/client-0-0-0-stderr-0.log", "2024-12-07-18-14-22/2024-12-07-18-15-55/2024-12-07-18-15-56/out/client-0-0/client-0-0-0-stderr-0.log"]

'''
for f in files:
    with open(f) as my_file:
        sendPropose = []
        readReply = []
        # sendpropose = %v, readReply = %v
        for line in my_file:
            if "sendpropose" in line:
                lines = line.split(",")
                sp = lines[0].split(" ")[-1]
                rp = lines[1].split(" ")[-1]
                if rp[-3:-1] == "ms":
                    readReply.append(float(rp[:-3]) * 1000)
                else:
                    readReply.append(float(rp[:-3]))

                if sp[-2:] == "ms":
                    sendPropose.append(float(sp[:-2]) * 1000)
                else:
                    sendPropose.append(float(sp[:-2]))
        
        print(f"Avg sp = {takeavg(sendPropose)} and median = {takemedian(sendPropose)}")
        print(f"Avg rp = {takeavg(readReply)} and median = {takemedian(readReply)}")
'''

files = ["2024-12-07-18-40-10/2024-12-07-18-41-40/2024-12-07-18-41-40/out/server-0/server-0-0-stderr-0.log", "2024-12-07-18-43-02/2024-12-07-18-44-35/2024-12-07-18-44-35/out/server-0/server-0-0-stderr-0.log",
        "2024-12-07-18-43-02/2024-12-07-18-44-35/2024-12-07-18-44-35/out/server-1/server-1-0-stderr-0.log",
        "2024-12-07-18-43-02/2024-12-07-18-44-35/2024-12-07-18-44-35/out/server-2/server-2-0-stderr-0.log",
        "2024-12-07-18-43-02/2024-12-07-18-44-35/2024-12-07-18-44-35/out/server-3/server-3-0-stderr-0.log",
        "2024-12-07-18-43-02/2024-12-07-18-44-35/2024-12-07-18-44-35/out/server-4/server-4-0-stderr-0.log",
        "2024-12-07-18-43-02/2024-12-07-18-44-35/2024-12-07-18-44-35/out/server-5/server-5-0-stderr-0.log",
        "2024-12-07-18-43-02/2024-12-07-18-44-35/2024-12-07-18-44-35/out/server-6/server-6-0-stderr-0.log",
        "2024-12-07-18-43-02/2024-12-07-18-44-35/2024-12-07-18-44-35/out/server-7/server-7-0-stderr-0.log",
        "2024-12-07-18-43-02/2024-12-07-18-44-35/2024-12-07-18-44-35/out/server-8/server-8-0-stderr-0.log"]
for f in files:
    with open(f) as my_file:
        t = []
        # reply for command 104 at took time
        for line in my_file:
            if "reply for command" in line:
                lines = line.split("time")
                t.append(float(lines[-1][:-1]))

        print(f"Avg time = {takeavg(t)}ns and median = {takemedian(t)}ns")
