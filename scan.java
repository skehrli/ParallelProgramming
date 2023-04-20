import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;

public class ParallelPrefix {
  static int[] sums;

  // Sequential prefix algorithm for testing
  public static int[] prefixSequential(int[] arr) {
    int[] res = new int[arr.length];
    res[0] = arr[0];
    for (int i = 1; i < arr.length; i++) {
      res[i] = arr[i] + res[i - 1];
    }
    return res;
  }

  // Parallel prefix algorithm
  public static int[] prefixParallel(int[] arr) {
    ForkJoinPool pool = new ForkJoinPool();
    int[] res = new int[arr.length];
    sums = new int[arr.length * 2];
    pool.invoke(new FirstPass(arr, res, 0, arr.length, 0));
    pool.invoke(new SecondPass(res, 0, 0, arr.length, 0));
    pool.shutdown();
    return res;
  }

  // first pass task
  static class FirstPass extends RecursiveTask<Integer> {
    private final int CUTOFF = 500;
    private int[] arr, res;
    private int start, end, id;

    FirstPass(int[] arr, int[] res, int start, int end, int id) {
      this.arr = arr;
      this.res = res;
      this.start = start;
      this.end = end;
      this.id = id;
    }

    protected Integer compute() {
      if (end - start <= CUTOFF) {
        int sum = arr[start];
        res[start] = arr[start];
        for (int i = start + 1; i < end; i++) {
          res[i] = res[i - 1] + arr[i];
          sum += arr[i];
        }
        sums[id] = sum;
        return sum;
      } else {
        int mid = (start + end) / 2;

        FirstPass left = new FirstPass(arr, res, start, mid, 2 * id + 1);
        FirstPass right = new FirstPass(arr, res, mid, end, 2 * id + 2);

        left.fork();
        right.fork();
        int leftSum = left.join();
        int rightSum = right.join();

        sums[id] = leftSum + rightSum;

        return leftSum + rightSum;
      }
    }
  }

  static class SecondPass extends RecursiveAction {
    private final int CUTOFF = 500;
    private int[] res;
    private int start, end, correction, id;

    public SecondPass(int[] res, int correction, int start, int end, int id) {
      this.res = res;
      this.correction = correction;
      this.start = start;
      this.end = end;
      this.id = id;
    }

    protected void compute() {
      if (end - start <= CUTOFF) {
        for (int i = start; i < end; i++) {
          res[i] += correction;
        }
      } else {
        int mid = (start + end) / 2;

        SecondPass left = new SecondPass(res, correction, start, mid, 2 * id + 1);
        SecondPass right = new SecondPass(res, correction + sums[2 * id + 1], mid, end, 2 * id + 2);
        left.fork();
        right.fork();
        left.join();
        right.join();
      }
    }
  }

  // Test the algorithm
  public static void main(String[] args) {
    int length = 10000;
    int[] arr = new int[length];
    Random random = new Random();
    for (int i = 0; i < length; i++) {
      arr[i] = random.nextInt(100); // generate random numbers between 0 and 99
    }

    int[] expected = new int[length];

    int[] res = new int[length];

    res = prefixParallel(arr);
    expected = prefixSequential(arr);
    int count = compareArrays(res, expected);
    if (count == 0) {
      System.out.println("The arrays are the same.");
    } else {
      System.out.println("The arrays differ in " + count + " elements.");
    }
  }

  public static int compareArrays(int[] arr1, int[] arr2) {
    int count = 0;
    if (arr1.length != arr2.length) {
      throw new IllegalArgumentException("The arrays have different lengths.");
    }
    for (int i = 0; i < arr1.length; i++) {
      if (arr1[i] != arr2[i]) {
        count++;
      }
    }
    return count;
  }
}
