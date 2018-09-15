package com.tpmms.controller;

import com.tpmms.service.FileService;
import com.tpmms.service.TpmmsService;
import com.tpmms.utility.CommonUtility;
import com.tpmms.utility.FileConstants;

public class TpmmsDataController {

	public static void main(String[] args) {

		try {
			int sortIO = 0;
			final FileService fileService = new FileService();
			fileService.deleteOldFiles(FileConstants.SUBLISTS_FILE_PATH);
			fileService.deleteOldFiles(FileConstants.OUTPUT_FILE_PATH);
			
			System.out.println("####==================== WELCOME TO TPMMS CONSOLE ====================####");
			System.out.println("Memory Size: " + CommonUtility.getMemorySizeInMB() + " MB\n");
			
			System.out.println("************ Relation T1 ************");
			final long startTimeT1 = System.nanoTime();
			TpmmsService relation1 = new TpmmsService(FileConstants.INPUT_FILE_PATH + FileConstants.RELATION_T1);
			System.out.println("Phase 1: ");
			sortIO += relation1.tpmmsSort();
			System.out.println("Phase 2: ");
			sortIO += relation1.tpmmsMerge();
			final long endTimeT1 = System.nanoTime();
			relation1 = null ;
			System.gc();
			
			System.out.println("\n************ Relation T2 ************");
			final long startTimeT2 = System.nanoTime();
			TpmmsService relation2 = new TpmmsService(FileConstants.INPUT_FILE_PATH + FileConstants.RELATION_T2);
			System.out.println("Phase 1: ");
			sortIO += relation2.tpmmsSort();
			System.out.println("Phase 2: ");
			sortIO += relation2.tpmmsMerge();
			final long endTimeT2 = System.nanoTime();
			
			System.out.println("\n************** Complete Sort Result ***************");
			System.out.println("Sort Disk I/O : " + sortIO);
			System.out.println("Execution time: " + (float) (endTimeT1 + endTimeT2  - startTimeT1 - startTimeT2) / 1000000000 + " seconds");

			System.out.println("\n************** Bag Difference(T1, T2) Result ***************");
			final long startTimeforBag = System.nanoTime();
			final int bagIO = relation2.getBagDifferenceCount();
			final long endTimeforBag = System.nanoTime();
			System.out.println("Bag Diff. Read Count : " + bagIO);
			System.out.println("Bag Diff. Execution time: " + (float) (endTimeforBag - startTimeforBag) / 1000000000 + " seconds");
			
			System.out.println("\n************** Final Result ***************");
			System.out.println("Total Disk I/O : " + (sortIO + bagIO));
			System.out.println("Total Execution time: " + (float) (endTimeT1 + endTimeT2 + endTimeforBag - startTimeT1 - startTimeT2 - startTimeforBag) / 1000000000 + " seconds");
			relation2 = null;
			System.gc();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
