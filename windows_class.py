import collections

"""
A class that can perform a function over a group of numbers,
where each number is added one number at a time.  This can
produce running sums, products, averages, geometric means, and
other interesting functions given proper values of add_fn
and del_fn (for adding a new value and deleting an old one resp.)
"""
class Aggregator(object):
    def __init__(self, initial_val, add_fn, del_fn):
        self.initial_val = initial_val
        self.add_fn = add_fn
        self.del_fn = del_fn
        self.count = 0
        self.val = self.initial_val

    def __str__(self):
        return "Aggregate.  Initial_Val = "+str(self.initial_val)+", "+ \
                "Add_Val = "+str(self.add_fn) + ", "+ \
                "Del_Val = "+str(self.del_fn) + ", "+ \
                "Total number of values = "+str(self.count)+", "+ \
                "Current Value = "+str(self.val)

    #takes the aggregate and adds a value
    #for example, for averaging 3 into (1,2)
    #we would have self.add_val = (self.val*self.count+new_val)/(self.count+1)
    #or (1.5*2+3)/(2+1) = 6/3 = 2
    def add_val(self, val):
        self.val = self.add_fn(self.val, val, self.count)
        self.count+=1

    #takes the current aggregate and removes a value
    #for example, if the removing 3 from (1,2,3)
    #we would have self.del_val = (self.val*self.count-old_val)/(self.count-1)
    #or (2*3-3)/(3-1) = 3/2 = 1.5
    def del_val(self, val):
        self.val = self.del_fn(self.val, val, self.count)
        self.count-=1 

    def get_val(self):
        return self.val

    def get_count(self):
        return self.count

    def clear(self):
        self.count = 0
        self.val = self.initial_val

"""
a = Aggregator(1, (lambda x,y,count: x*y), (lambda x,y,count: x/y))

#testing - include 'print a' statements as desired to check Aggregator state
a.add_val(15)
a.add_val(-2)
a.add_val(0)
a.add_val(6)
a.del_val(15)
a.del_val(-2)
a.clear()
a.add_val(45)
a.del_val(15)
a.clear()
"""


#TO-DO: current implementation assumes in-order receipt of data - make more robust
class Window(object):
    #window in the form of [(id1, val), (id2, val2), ...]
    #a size-based window may be of the form [(11, x), (12, y), ...]
    #while a time-based window may be like [(1001134, x), (1012094, y), ...]

    #for disjoint windows, set win_width=win_skip
    def __init__(self, win_init, win_width, win_skip, aggregate):
        self.window = collections.deque()
        self.window_min = win_init
        self.window_width = win_width
        self.window_skip = win_skip
        #this should be an Aggregator instance
        self.aggregate = aggregate


    def add_val(self, val):
        #if we've finished our old interval, we need
        #to save that old value and set up for the new interval
        if(val[0]<self.window_min):
            #this is an old value -- we need to notify the sender
            #to handle it itself
            return val
        self.update(val[0])
        self.window.append(val)
        self.aggregate.add_val(val[1])

    #deletion automatically deletes the oldest first
    #don't think this is necessary
    def del_val(self):
        old_val = self.window.popleft()
        self.aggregate.del_val(old_val[1])

    def get_next_element(self):
        try:
            toReturn = self.window.popleft()
            self.window.appendleft(toReturn)
            return toReturn
        except IndexError as e:
            return None

    def update_min(self, new_min):
        #the -1 is confusing, but consider this (with window_width=3):
        #if I say that 7 is a new max, I would want a window [5, 6, 7]
        #but if I want 4 as a new min, I would want [4, 5, 6], not [5, 6, 7]
        #So the -1 comes from min being inclusive and max being inclusive.
        return self.update(new_min+self.window_width-1)

    #invariant - the values in the window all correspond to one interval
    #For a window of length 3 initially at [1, 2, 3] with min=1, setting 
    #new_min=3 will return the aggregate of the windows [1,2,3] and [2,3],
    #and leave the window with the lone value [3]
    def update(self, new_max):
        #new value timestamp/id is outside the current tentative window
        toReturn = None
        if(new_max>=self.window_min+self.window_width):
            toReturn = []

            #clear out any empty intervals betweent the old
            #data and the newly added point
            while(self.window_min+self.window_width<=new_max):
                #all the current values make up a complete window
                #aggregate, discard old values, and advamce the window's beginning
                toReturn.append([self.window_min, self.aggregate.get_val(), self.aggregate.get_count()])
                self.window_min += self.window_skip
                while( (self.get_next_element() is not None) and (self.get_next_element()[0]<self.window_min) ):
                    self.del_val()
        return toReturn



    #calculates the intermediate result of the 
    #desired aggregate function over the     
    def calc_aggregate(self):
        return self.aggregate.get_val()

"""
for testing Window
b = Aggregator( 0, (lambda x,y,count: (x*count+y)/(count+1.0)), (lambda x,y,count: 0 if count==1 else (x*count-y)/(count-1.0)) )
c = Window(1, 3, 1, b)
c.add_val((1,15))
print c.calc_aggregate()
c.add_val((2,5))
print c.calc_aggregate()
c.add_val((3,-20))
print c.calc_aggregate()
c.add_val((4,30))
print c.calc_aggregate()
print c.update_min(4)
print c.calc_aggregate()
print c.aggregate
"""