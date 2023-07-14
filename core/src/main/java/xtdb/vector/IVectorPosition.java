package xtdb.vector;

public interface IVectorPosition {
    int getPosition();
    void setPosition(int position);

    int getPositionAndIncrement();

    static IVectorPosition build() {
        return build(0);
    }

    static IVectorPosition build(int initialPosition) {
        return new IVectorPosition() {
            private int position = initialPosition;

            @Override
            public int getPosition() {
                return position;
            }

            @Override
            public void setPosition(int position) {
                this.position = position;
            }

            @Override
            public int getPositionAndIncrement() {
                int position = this.position;
                this.position++;
                return position;
            }
        };
    }
}
