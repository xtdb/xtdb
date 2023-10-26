package xtdb.vector;

public interface IVectorPosition {
    int getPosition();
    void setPosition(int position);

    default int getPositionAndIncrement() {
        int position = getPosition();
        this.setPosition(position + 1);
        return position;
    }

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
        };
    }
}
