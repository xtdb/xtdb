package core2.vector;

public interface IWriterPosition {
    int getPosition();
    void setPosition(int position);

    int getPositionAndIncrement();

    static IWriterPosition build() {
        return build(0);
    }

    static IWriterPosition build(int initialPosition) {
        return new IWriterPosition() {
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
