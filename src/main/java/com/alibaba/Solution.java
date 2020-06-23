/*
 * Author github: https://github.com/zs-neo
 * Author Email: 2931622851@qq.com
 */
package com.alibaba;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author zhousheng
 * @version 1.0
 * @since 2020/6/21 9:16
 */
public class Solution {
	
	int[][] dir = {{1, 0}, {-1, 0}, {0, 1}, {0, -1}};
	
	public int find(int[][] matrix, int x, int y, int[][] res, int row, int col) {
		if (res[x][y] != 0) {
			return res[x][y];
		}
		for (int i = 0; i < 4; i++) {
			int newx = x + dir[i][0];
			int newy = y + dir[i][1];
			if (newx >= 0 && newx < row && newy >= 0 && newy < col) {
				if (matrix[x][y] < matrix[newx][newy]) {
					res[x][y] = Math.max(find(matrix, newx, newy, res, row, col), res[x][y]);
				}
			}
		}
		++res[x][y];
		return res[x][y];
	}
	
	public int longestIncreasingPath(int[][] matrix) {
		if (matrix.length == 0) {
			return 0;
		}
		int ans = -1;
		int row = matrix.length;
		int col = matrix[0].length;
		int[][] res = new int[row][col];
		for (int i = 0; i < row; i++) {
			for (int j = 0; j < col; j++) {
				ans = Math.max(ans, find(matrix, i, j, res, row, col));
			}
		}
		for (int i = 0; i < row; i++) {
			for (int j = 0; j < col; j++) {
				System.out.print(res[i][j] + " ");
			}
			System.out.println();
		}
		return ans;
	}
	
	public static void main(String[] args) {
		Solution solution = new Solution();
		int[][] a = {{9, 9, 4},
				{6, 6, 8},
				{2, 1, 1}};
		int[][] b = {{3, 4, 5},
				{3, 2, 6},
				{2, 2, 1}};
		System.out.println(solution.longestIncreasingPath(a));
		System.out.println(solution.longestIncreasingPath(b));
	}
	
}
