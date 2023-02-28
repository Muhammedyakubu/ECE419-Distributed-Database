package shared.messages;

public class Pair<T, t> {
        T p1;
        t p2;
        Pair()
        {
            //default constructor
        }
        void setValue(T a, t b)
        {
            this.p1 = a;
            this.p2 = b;
        }

        public T getFirst()
        {
            return p1;
        }

        public t getSecond()
        {
            return p2;
        }
        Pair getValue()
        {
            return this;
        }


}
