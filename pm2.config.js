// @ts-ignore
module.exports = {
    name: 'history',
    script: 'dist/index.js',
    max_memory_restart: '2000M',
    exec_mode: "cluster",
    instances: 'max',
    instance_var: 'INSTANCE_ID',
}