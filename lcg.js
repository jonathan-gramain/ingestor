// https://en.wikipedia.org/wiki/Linear_congruential_generator
//
// random generation is much better with a m having a large number of repeated prime
// factors (such as powers of 2)
function lcgInit(m, seed) {
    const power = Math.ceil(Math.log2(m));
    const m2 = 2 ** power;

    return {
        m,
        m2,
        n: seed % m,
        iter: 0,
    };
}

function lcgGen(state) {
    const { m, m2, n, iter } = state;
    const a = 1664525;
    const c = 1013904223;

    let nextN = n;
    do {
        nextN = (a*nextN + c) % m2;
    } while (nextN >= m);
    Object.assign(state, { m, m2, n: nextN, iter: iter + 1 });
    return nextN;
}

module.exports = {
    lcgInit,
    lcgGen,
};
