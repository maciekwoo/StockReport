from data_processing_ml import PreProcessedData
import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf
from pandas.plotting import register_matplotlib_converters
from matplotlib.ticker import MaxNLocator

register_matplotlib_converters()

n_steps = 20
n_inputs = 1
n_neurons = 100
n_outputs = 1
n_iterations = 2000
batch_size = 50
learning_rate = 0.001

gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=0.75)


def reset_graph(seed=42):
    tf.reset_default_graph()
    tf.set_random_seed(seed)
    np.random.seed(seed)


my_data = PreProcessedData(ticker="WSE/CDPROJEKT", start_dt='2019-01-01', end_dt='2019-08-01')

t_instance = my_data.t_instance(rand_id=364, days=20)
t_instance_dates = my_data.t_instance_dates(rand_id=364, days=20)

# ML MAGIC
X = tf.placeholder(tf.float32, [None, n_steps, n_inputs])
y = tf.placeholder(tf.float32, [None, n_steps, n_outputs])

cell = tf.contrib.rnn.OutputProjectionWrapper(
    tf.nn.rnn_cell.BasicRNNCell(num_units=n_neurons, activation=tf.nn.relu),
    output_size=n_outputs)

outputs, states = tf.nn.dynamic_rnn(cell, X, dtype=tf.float32)

loss = tf.reduce_mean(tf.square(outputs - y))  # MSE
optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)
training_op = optimizer.minimize(loss)

init = tf.global_variables_initializer()

saver = tf.train.Saver()

with tf.Session(config=tf.ConfigProto(gpu_options=gpu_options)) as sess:
    init.run()
    for iteration in range(n_iterations):
        X_batch, y_batch = my_data.next_batch(batch_size, n_steps)
        sess.run(training_op, feed_dict={X: X_batch, y: y_batch})
        if iteration % 100 == 0:
            mse = loss.eval(feed_dict={X: X_batch, y: y_batch})
            print(iteration, "\tMSE:", mse)

    saver.save(sess, "./my_time_series_model")

with tf.Session() as sess:
    saver.restore(sess, "./my_time_series_model")
    X_new = t_instance
    y_pred = sess.run(outputs, feed_dict={X: X_new})

# Plot model testing outputs
fig, ax = plt.subplots(figsize=(12, 6))
ax.set_xlabel('Time')
ax.set_ylabel('Price')
ax.set_title('Testing the model')

a = np.arange(20)

ax.plot(a[:-1], t_instance[0][:-1], "-", markersize=10, label="instance")
ax.plot(a[1:], t_instance[0][1:], "*", markersize=10, label="target")
ax.plot(a[1:], y_pred[0][1:], "r--", markersize=10, label="prediction")
fig.legend(loc="upper left")

ax.xaxis.set_major_locator(MaxNLocator(integer=True))

plt.show()
