from random import choice, shuffle
import functools
import os

class Op(object):
    def __init__(self, typ, key):
        self.typ = typ
        self.key = key
        self.pid = 0 
        self.index = 0
        self.version = None
        self.d = set()
        self.conflict = None
        self.val = None
        self.null_conflict = None
        self.RO_dep = False

    def __str__(self):
        if self.typ:
            return "W(" + str(self.key) + ")"
        else:
            return "R(" + str(self.key) + ")"
    def __repr__(self):
        if self.typ:
            return "W(" + str(self.key) + ")"
        else:
            return "R(" + str(self.key) + ")"
    def get_key(self):
        return self.key
    def is_write(self):
        return self.typ
    
    def put_index(self, i):
        self.index = i 
    def put_pid(self, pid):
        self.pid = pid
    def put_version(self, version):
        self.version = version 
    def get_version(self):
        return self.version
    def get_pid(self):
        return self.pid
        
    def add_dependencies(self,d):
        self.d.update(d)
    def get_dependencies(self):
        return self.d

    def set_val(self, v):
        self.val = v
    def get_val(self):
        return self.val

    def set_conflict(self, op):
        if self.conflict == None:
            self.conflict = []
        self.conflict += [op]
    def get_conflicts(self):
        if self.conflict == None:
            return []
        return self.conflict

    def set_null_conflict(self, op):
        if self.null_conflict == None:
            self.null_conflict = []
        self.null_conflict += [op]
    def get_null_conflicts(self):
        if self.null_conflict == None:
            return []
        return self.null_conflict

class R(Op):
    def __init__(self, key):
        super().__init__(0, key)
    def __str__(self):
        extra = "" if self.val == None else "="+str(self.val)
        return "R(" + str(self.key) + ")" + extra
    def __repr__(self):
        extra = "" if self.val == None else "="+str(self.val)
        return "R(" + str(self.key) + ")" + extra
        
class W(Op):
    def __init__(self, key, val):
        super().__init__(1, key)
        self.val = val
    def __str__(self):
        return "W(" + str(self.key) + "="+str(self.val) +")"
    def __repr__(self):
        return "W(" + str(self.key) + "="+str(self.val) +")"
        
class Batch(object):
    def __init__(self, ops, pid):
        self.ops = ops
        self.client = pid
        i = 0
        for op in ops:
            op.put_index(i)
            op.put_pid(pid)
            i+=1
    def __str__(self):
        s = "Client_" + str(self.client) + ":  "
        offset = len(s)
        i = 0
        for op in self.ops:
            if i > 0:
                s+="\n"+" "*i*2 + " "*offset
            s+= "|---" + str(op)+ "---|"
            if len(op.d) > 0:
                deps = "{"
                for d in op.d:
                    deps += str(d.get_key())+"."+str(d.get_version())+"."+str(d.get_pid())+", "
                deps = deps[:-2]
                deps += "}"
                s+="  d = "+deps
            i+=1
        return s
    def get_ops(self):
        return self.ops


##########################################################

class Client(object):
    def __init__(self, pid):
        self.pid = pid
    def generate_batch(self, n, keys):
        L = []
        for i in range(n):
            if choice([0,1]):
                v = self.pid+i
                L.append(W(choice(keys),v))
            else:
                L.append(R(choice(keys)))
        b = Batch(L, self.pid)
        self.batch = b
        return b
    def put_batch(self, b):
        self.batch = Batch(b, self.pid)

    def trickle_dependencies(self):
        prev_d = set()
        for op in self.batch.get_ops():
            op.add_dependencies(prev_d)
            prev_d.update([op])
            prev_d.update(op.get_dependencies())

    def detect_conflicts(self):
        for op in self.batch.get_ops():
            for e in op.d:
                if (e.get_key() == op.get_key()) and \
                    (e.get_version() >= op.get_version()) and \
                        (e.get_pid() > op.get_pid()):
                    
                    # If i'm a read being swapped with a read with no writes between us
                    if (e.get_version() == op.get_version()) and \
                        (not e.is_write() and not op.is_write()):
                        op.set_null_conflict(e)

                    if (e.RO_dep):
                        op.set_null_conflict(e) # Checking if this is correct lol

                    else: op.set_conflict(e)
                    
            

##########################################################

class Shard(object):
    def __init__(self, keys):
        self.keys = keys
        self.log = []
        self.version = choice([i for i in range(20)])
        self.v0 = self.version
        self.pids = set()
    
    def add_op(self, op):
        k = op.get_key()
        if not k in self.keys:
            # print("key " + k + " not held at this shard")
            return
        self.log.append(op)
        self.pids.add(op.get_pid())
    
    def add_ops(self, ops):
        if type(ops) == Batch:
            ops = ops.get_ops()
        for op in ops:
            self.add_op(op)
            
    def __str__(self):
        s = "Shard " + str(self.keys) + " (" +str(self.v0)+")"+ "\n"
        i = 0
        for op in self.log:
            s += str(i) + " "
            if op.is_write():
                lhs = "|--"
                rhs = "--|"
            else:
                lhs = "|---"
                rhs = "---|"
            ver = "" if op.version == None else ",version="+str(op.version)
            conf = "" if len(c := op.get_conflicts()) == 0 else ",conflict="+str(c)+"<-----SWAP"
            nconf = "" if len(nc := op.get_null_conflicts()) == 0 else ",null conflicts="+str(nc)+"<-----SWAP!X!X!X!X!X!"
            deps = ""
            if len(op.d) > 0:
                deps = "{"
                for d in op.d:
                    deps += str(d.get_key())+"."+str(d.get_version())+"."+str(d.get_pid())+", "
                deps = deps[:-2]
                deps += "}"
            s += lhs + str(op) + rhs + "   pid="+str(op.pid)+",bi="+str(op.index)+\
                                        ver+deps+conf+nconf+"\n"
            i += 1
        return s.strip()
    def __repr__(self):
        return "Shard " + str(self.keys)
    def shuffle_shard(self):
        # print("BEFORE", self.log)
        shuffle(self.log)
        # print("AFTER", self.log)
        for p in self.pids:
            B = [(i, self.log[i]) if self.log[i].get_pid()==p else None for i in range(len(self.log))]
            B = list(filter(lambda x: x != None, B))
            A = [e[1] for e in B]
            A.sort(key=lambda x: x.index)
            for i in range(len(B)):
                self.log[B[i][0]] = A[i]
        # print("FINAL", self.log)
        # print("**************")
    
    def assign_versions(self):
        prev_val = 0
        for op in self.log:
            if op.is_write():
                self.version+=1
                prev_val = op.get_val()
            else:
                op.set_val(prev_val)
            op.put_version(self.version)
    
    def trickle_dependencies(self):
        prev_d = set()
        all_RO = True
        for op in self.log:
            # Optimization
            if all_RO and (op.is_write()):
                all_RO = False
                tag_RO(prev_d, True)
            if (all_RO):
                # Tag them as special so that conflicts are nullified
                tag_RO(prev_d)

            op.add_dependencies(prev_d)
            # prev_d.update([op]) don't need the ops, just deps
            prev_d.update(op.get_dependencies())
            
    # True if has conflicts, False if no conflicts
    def perform_swaps(self):
        for op in self.log:
            if len(op.get_conflicts()) > 0:
                # print("!!!!!Conflict detected on shard %s D:" % str(self.keys))
                return True
            if len(op.get_null_conflicts()) > 0:
                return True
        # print("No conflicts detected on shard %s :D" % str(self.keys))
        return False
            
def tag_RO(s, undo=False):
    what = False if undo else True
    for op in s:
        op.RO_dep = what
##########################################################

class Execution(object):
    def __init__(self, num_clients, batch_size, keys):
        self.num_clients = num_clients
        self.shards = []
        for k in keys:
            self.shards.append(Shard(k))
        self.clients = []
        for i in range(num_clients):
            c = Client(100*(i+1))
            c.generate_batch(batch_size, keys)
            self.clients.append(c)
            for s in self.shards:
                s.add_ops(c.batch)
        for s in self.shards:
            s.shuffle_shard()
    
    def __str__(self):
        s = ""
        for c in self.clients:
            s += str(c.batch)
            s += "\n"
        s += "----------------------------------------------------------------------------------------------\n"
        for sh in self.shards:
            s += str(sh)
            s += "\n\n"
        return s
    def round1(self):
        for s in self.shards:
            s.assign_versions()
        for c in self.clients:
            c.trickle_dependencies()
    def round2(self):
        # for c in self.clients:
        #     c.trickle_dependencies()
        # moved this up into round 1
        for s in self.shards:
            s.trickle_dependencies()
    def round3(self):
        for c in self.clients:
            c.trickle_dependencies()
            c.detect_conflicts() ### ******
        found_conflicts = False
        for s in self.shards:
            r = s.perform_swaps()
            if r: found_conflicts = True
        return found_conflicts
    
    def run(self):
#         print("__________________________________________________________________________\
# \n__________________________INITIAL EXECUTION______________________________")
#         print(self)
        self.round1()
#         print("__________________________________________________________________________\
# \n_______________________EXECUTION AFTER ROUND 1___________________________")
#         print(self)
        self.round2()
#         print("__________________________________________________________________________\
# \n_______________________EXECUTION AFTER ROUND 2___________________________")
#         print(self)
        r = self.round3()
#         print("__________________________________________________________________________\
# \n_______________________EXECUTION AFTER ROUND 3____________________________")
        # print(self)

        return r

##########################################################
class ExactExecution(Execution):
    def __init__(self, clients, shards, add_ops=False, shuffle=False):
        self.num_clients = len(clients)
        self.shards = shards
        self.clients = clients
        if add_ops:
            for c in self.clients:
                for s in self.shards:
                    s.add_ops(c.batch)
            for s in self.shards:
                s.shuffle_shard()
        elif shuffle:
            for s in self.shards:
                s.shuffle_shard()
##########################################################
os.system('cls')
print("\n\n\n\n***********************************************************************************************\n\
***********************************************************************************************\n\n\n")

def simple2RWTest():
    # Test 1
    c1 = Client(100)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(200)
    c2o1 = R("Y")
    c2o2 = R("X")
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c1o1, c2o2])
    s2.add_ops([c2o1, c1o2])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r==False)

    # Test 2
    c1 = Client(100)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(200)
    c2o1 = R("Y")
    c2o2 = R("X")
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c2o2, c1o1])
    s2.add_ops([c2o1, c1o2])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r==False)

    # Test 3
    c1 = Client(100)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(200)
    c2o1 = R("Y")
    c2o2 = R("X")
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c2o2, c1o1])
    s2.add_ops([c1o2, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

    # Test 4
    c1 = Client(100)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(200)
    c2o1 = R("Y")
    c2o2 = R("X")
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c1o1, c2o2])
    s2.add_ops([c1o2, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r==False)

def simple2WOTest():
    # Test 1
    c1 = Client(100)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(200)
    c2o1 = W("Y", 2)
    c2o2 = W("X", 2)
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c1o1, c2o2])
    s2.add_ops([c2o1, c1o2])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r==False)

    # Test 2
    c1 = Client(100)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(200)
    c2o1 = W("Y", 2)
    c2o2 = W("X", 2)
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c2o2, c1o1])
    s2.add_ops([c2o1, c1o2])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r==False)

    # Test 3
    c1 = Client(100)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(200)
    c2o1 = W("Y", 2)
    c2o2 = W("X", 2)
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c2o2, c1o1])
    s2.add_ops([c1o2, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

    # Test 4
    c1 = Client(100)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(200)
    c2o1 = W("Y", 2)
    c2o2 = W("X", 2)
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c1o1, c2o2])
    s2.add_ops([c1o2, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r==False)

    # Swap client PIDS

    # Test 5
    c1 = Client(200)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(100)
    c2o1 = W("Y", 2)
    c2o2 = W("X", 2)
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c1o1, c2o2])
    s2.add_ops([c2o1, c1o2])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r==False)

    # Test 6
    c1 = Client(200)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(100)
    c2o1 = W("Y", 2)
    c2o2 = W("X", 2)
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c2o2, c1o1])
    s2.add_ops([c2o1, c1o2])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r==False)

    # Test 7
    c1 = Client(200)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(100)
    c2o1 = W("Y", 2)
    c2o2 = W("X", 2)
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c2o2, c1o1])
    s2.add_ops([c1o2, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

    # Test 8
    c1 = Client(200)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c2 = Client(100)
    c2o1 = W("Y", 2)
    c2o2 = W("X", 2)
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c1o1, c2o2])
    s2.add_ops([c1o2, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r==False)

def basic4ROTest():
    # Test 1
    c1 = Client(100)
    c1o1 = W("A", 1)
    c1o2 = W("B", 1)
    c1o3 = W("C", 1)
    c1o4 = W("D", 1)
    c2 = Client(200)
    c2o1 = W("D", 2)
    c2o2 = W("C", 2)
    c2o3 = W("B", 2)
    c2o4 = W("A", 2)
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2, c1o3, c1o4])
    c2.put_batch([c2o1, c2o2, c2o3, c2o4])
    s1 = Shard("A")
    s2 = Shard("B")
    s3 = Shard("C")
    s4 = Shard("D")
    shards = [s1, s2, s3, s4]
    s1.add_ops([c1o1, c2o4])
    s2.add_ops([c2o3, c1o2])
    s3.add_ops([c2o2, c1o3])
    s4.add_ops([c1o4, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

    # Test 2
    c1 = Client(200)
    c1o1 = W("A", 1)
    c1o2 = W("B", 1)
    c1o3 = W("C", 1)
    c1o4 = W("D", 1)
    c2 = Client(100)
    c2o1 = W("D", 2)
    c2o2 = W("C", 2)
    c2o3 = W("B", 2)
    c2o4 = W("A", 2)
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2, c1o3, c1o4])
    c2.put_batch([c2o1, c2o2, c2o3, c2o4])
    s1 = Shard("A")
    s2 = Shard("B")
    s3 = Shard("C")
    s4 = Shard("D")
    shards = [s1, s2, s3, s4]
    s1.add_ops([c1o1, c2o4])
    s2.add_ops([c2o3, c1o2])
    s3.add_ops([c2o2, c1o3])
    s4.add_ops([c1o4, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

def basic3clientBoundedReissuingTest():
    # Test 1
    c1 = Client(100)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c1o3 = W("Z", 1)
    c2 = Client(200)
    c2o1 = R("Y")
    c2o2 = R("X")
    c2o3 = R("Z")
    c3 = Client(300)
    c3o1 = W("Z", 2)
    c3o2 = W("X", 2)
    c3o3 = W("Y", 2)
    clients = [c1, c2, c3]
    c1.put_batch([c1o1, c1o2, c1o3])
    c2.put_batch([c2o1, c2o2, c2o3])
    c3.put_batch([c3o1, c3o2, c3o3])
    s1 = Shard("X")
    s2 = Shard("Y")
    s3 = Shard("Z")
    shards = [s1, s2, s3]
    s1.add_ops([c2o2, c1o1, c3o2])
    s2.add_ops([c1o2, c2o1, c3o3])
    s3.add_ops([c1o3, c2o3, c3o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

    # Test 2
    c1 = Client(200)
    c1o1 = W("X", 1)
    c1o2 = W("Y", 1)
    c1o3 = W("Z", 1)
    c2 = Client(100)
    c2o1 = R("Y")
    c2o2 = R("X")
    c2o3 = R("Z")
    c3 = Client(300)
    c3o1 = W("Z", 2)
    c3o2 = W("X", 2)
    c3o3 = W("Y", 2)
    clients = [c1, c2, c3]
    c1.put_batch([c1o1, c1o2, c1o3])
    c2.put_batch([c2o1, c2o2, c2o3])
    c3.put_batch([c3o1, c3o2, c3o3])
    s1 = Shard("X")
    s2 = Shard("Y")
    s3 = Shard("Z")
    shards = [s1, s2, s3]
    s1.add_ops([c2o2, c1o1, c3o2])
    s2.add_ops([c1o2, c2o1, c3o3])
    s3.add_ops([c1o3, c2o3, c3o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

def moreComplex3clientTest():
    # Test 1
    c1 = Client(100)
    c1o1 = R("Y")
    c1o2 = W("Z", 1)
    c2 = Client(200)
    c2o1 = R("Z")
    c2o2 = W("Y", 1)
    c2o3 = R("X")
    c3 = Client(300)
    c3o1 = W("X", 1)
    c3o2 = R("Y")
    c3o3 = W("Y", 2)
    clients = [c1, c2, c3]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2, c2o3])
    c3.put_batch([c3o1, c3o2, c3o3])
    s1 = Shard("X")
    s2 = Shard("Y")
    s3 = Shard("Z")
    shards = [s1, s2, s3]
    s1.add_ops([c2o3, c3o1])
    s2.add_ops([c2o2, c1o1, c3o2, c3o3])
    s3.add_ops([c1o2, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

    # Test 2
    c1 = Client(200)
    c1o1 = R("Y")
    c1o2 = W("Z", 1)
    c2 = Client(100)
    c2o1 = R("Z")
    c2o2 = W("Y", 1)
    c2o3 = R("X")
    c3 = Client(300)
    c3o1 = W("X", 1)
    c3o2 = R("Y")
    c3o3 = W("Y", 2)
    clients = [c1, c2, c3]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2, c2o3])
    c3.put_batch([c3o1, c3o2, c3o3])
    s1 = Shard("X")
    s2 = Shard("Y")
    s3 = Shard("Z")
    shards = [s1, s2, s3]
    s1.add_ops([c2o3, c3o1])
    s2.add_ops([c2o2, c1o1, c3o2, c3o3])
    s3.add_ops([c1o2, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

    # Test 3
    c1 = Client(100)
    c1o1 = R("Y")
    c1o2 = W("Z", 1)
    c2 = Client(300)
    c2o1 = R("Z")
    c2o2 = W("Y", 1)
    c2o3 = R("X")
    c3 = Client(200)
    c3o1 = W("X", 1)
    c3o2 = R("Y")
    c3o3 = W("Y", 2)
    clients = [c1, c2, c3]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2, c2o3])
    c3.put_batch([c3o1, c3o2, c3o3])
    s1 = Shard("X")
    s2 = Shard("Y")
    s3 = Shard("Z")
    shards = [s1, s2, s3]
    s1.add_ops([c2o3, c3o1])
    s2.add_ops([c2o2, c1o1, c3o2, c3o3])
    s3.add_ops([c1o2, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

    # Test 4
    c1 = Client(100)
    c1o1 = R("Y")
    c1o2 = W("Z", 1)
    c2 = Client(300)
    c2o1 = R("Z")
    c2o2 = W("Y", 1)
    c2o3 = R("X")
    c3 = Client(200)
    c3o1 = W("X", 1)
    c3o2 = R("Y")
    c3o3 = W("Y", 2)
    clients = [c1, c2, c3]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2, c2o3])
    c3.put_batch([c3o1, c3o2, c3o3])
    s1 = Shard("X")
    s2 = Shard("Y")
    s3 = Shard("Z")
    shards = [s1, s2, s3]
    s1.add_ops([c2o3, c3o1])
    s2.add_ops([c3o2, c2o2, c3o3, c1o1])
    s3.add_ops([c1o2, c2o1,])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

def shufflingComplexTest():
    for x in range(10):
        c1 = Client(100)
        c1o1 = R("Y")
        c1o2 = W("Z", 1)
        c2 = Client(300)
        c2o1 = R("Z")
        c2o2 = W("Y", 1)
        c2o3 = R("X")
        c3 = Client(200)
        c3o1 = W("X", 1)
        c3o2 = R("Y")
        c3o3 = W("Y", 2)
        clients = [c1, c2, c3]
        c1.put_batch([c1o1, c1o2])
        c2.put_batch([c2o1, c2o2, c2o3])
        c3.put_batch([c3o1, c3o2, c3o3])
        s1 = Shard("X")
        s2 = Shard("Y")
        s3 = Shard("Z")
        shards = [s1, s2, s3]
        s1.add_ops([c3o1, c2o3])
        s2.add_ops([c3o2, c3o3, c1o1, c2o2])
        s3.add_ops([c2o1, c1o2])
        e = ExactExecution(clients, shards, shuffle=True)
        r = e.run()
        print("has _conflicts = ", r)

def badCasesTest():
    c1 = Client(100)
    c1o1 = R("Y")
    c1o2 = W("Z", 1)
    c2 = Client(200)
    c2o1 = R("Z")
    c2o2 = W("Y", 1)
    c2o3 = R("X")
    c3 = Client(300)
    c3o1 = W("X", 1)
    c3o2 = R("Y")
    c3o3 = W("Y", 2)
    clients = [c1, c2, c3]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2, c2o3])
    c3.put_batch([c3o1, c3o2, c3o3])
    s1 = Shard("X")
    s2 = Shard("Y")
    s3 = Shard("Z")
    shards = [s1, s2, s3]
    s1.add_ops([c2o3, c3o1])
    s2.add_ops([c3o2, c2o2, c3o3, c1o1])
    s3.add_ops([c1o2, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)

def specialCasesTest():
    c1 = Client(100)
    c1o1 = R("D")
    c1o2 = W("B", 103)
    c2 = Client(200)
    c2o1 = R("B")
    c2o2 = R("D")
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("B")
    s2 = Shard("D")
    shards = [s1, s2]
    s1.add_ops([c1o2, c2o1])
    s2.add_ops([c2o2, c1o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)
    # print(e)

    c1 = Client(100)
    c1o1 = R("D")
    c1o2 = W("B", 101)
    c2 = Client(200)
    c2o2 = W("B", 201)
    c2o3 = R("D")
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o2, c2o3])
    s1 = Shard("B")
    s2 = Shard("D")
    shards = [s1, s2]
    s1.add_ops([c1o2, c2o2])
    s2.add_ops([c2o3, c1o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)
    # print(e)

    c1 = Client(200)
    c1o1 = W("X", 1)
    c1o2 = W("X", 2)
    c1o3 = W("Y", 1)
    c2 = Client(100)
    c2o1 = R("Y")
    c2o2 = R("X")
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2, c1o3])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c2o2, c1o1, c1o2])
    s2.add_ops([c1o3, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)
    # print(e)

    c1 = Client(100)
    c1o1 = W("Y", 1)
    c1o2 = W("X", 1)
    c1o3 = W("X", 2)
    c2 = Client(200)
    c2o1 = R("X")
    c2o2 = R("Y")
    clients = [c1, c2]
    c1.put_batch([c1o1, c1o2, c1o3])
    c2.put_batch([c2o1, c2o2])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c1o2, c1o3, c2o1])
    s2.add_ops([c2o2, c1o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)
    # print(e)

    c1 = Client(100)
    c1o1 = W("X", 1)
    c1o2 = R("Y")
    c3 = Client(300)
    c3o1 = W("Y", 2)
    c2 = Client(200)
    c2o1 = R("Y")
    c2o2 = W("X", 2)
    clients = [c1, c2, c3]
    c1.put_batch([c1o1, c1o2])
    c2.put_batch([c2o1, c2o2])
    c3.put_batch([c3o1])
    s1 = Shard("X")
    s2 = Shard("Y")
    shards = [s1, s2]
    s1.add_ops([c2o2, c1o1])
    s2.add_ops([c1o2, c3o1, c2o1])
    e = ExactExecution(clients, shards)
    r = e.run()
    assert(r)
    print(e)

def combinationsTest():
    # 1|    RW WR       !X!X!X
    # 2|    RW WW       XXXXXX
    # 3|    WW WW       XXXXXX
    # 4|    WW WR       XXXXXX

    # 5|    RR RR       !X!X!X
    # 6|    RR RW       !X!X!X
    # 7|    WR RW       !X!X!X
    # 8|    WR RR       !X!X!X

    # 9|    RR WR       !X!X!X
    #10|    RR WW       XXXXXX
    #11|    WR WW       XXXXXX
    #12|    WR WR       XXXXXX

    #13|    RW RR       !X!X!X
    #14|    RW RW       XXXXXX
    #15|    WW RW       XXXXXX
    #16|    WW RR       XXXXXX

    # don't matter(7) -- have only reads on 1 shard
    # matter(9) -- have at least 1 write on both shards

    # How to find?? 
    #   - if i'm a read and i'm being swapped with a read with 
    #     no writes between us

    #   - if i'm a write being swapped, but my dependencies came 
    #     from a read behind only reads
    return True

def runTests():
    # simple2RWTest()
    # simple2WOTest()
    # basic4ROTest()
    # basic3clientBoundedReissuingTest()
    # moreComplex3clientTest()
    # shufflingComplexTest()
    # badCasesTest()
    # specialCasesTest()
    # combinationsTest()

    for i in range(10):
        keys = ["A", "B", "C", "D"]
        e = Execution(2, 4, keys)
        if (e.run()):
            print("="*100)
            print(" "*30, "Case number", i+1)
            print(e)

runTests()
