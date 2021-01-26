
class Rectangle:

    def __init__(self,width,height):

        #calling setter inside the init method
        self.width=width
        self.height=height

    #use of python property decorator

    @property
    def width(self):
        return self._width

    @width.setter
    def width(self,val):
        if val <=0:
            raise ValueError("width must be positive")
        else:
            #declaring a private keyword for _width
            self._width=val


    @property
    def height(self):
        return self._height    
    @height.setter
    def height(self,val):
        if val<=0:
            raise ValueError("height must be positive")
        else:
            #declaring a private keyword here
            self._height=val
    

    #height=property(get_width,set_width)

    def __repr__(self):
        return 'Rectange: width={0} and height={1}'.format(self._width,self._height)
    
    def __str__(self):
        return 'Rectangle({0},{1})'.format(self._width,self._height)

    def __eq__(self,other):

        if isinstance(other,Rectangle):
            return self._height==other._height and self._width==other._width
        else:
            return False


if __name__ == "__main__":

    r1= Rectangle(10,20)
    r2= Rectangle(10,21)
    print(r1,r2)

    print(r1==r2)
    print(r1._width)
    r1.height=-39
    r1.width=-180 #trying to set the width for the r1 but it is not correct way to set the attributes and is known as the monkey patching
    #the correct way to implement the setter and getter within the python,

    print(r1)


    #what is garbage collection in python:
    

