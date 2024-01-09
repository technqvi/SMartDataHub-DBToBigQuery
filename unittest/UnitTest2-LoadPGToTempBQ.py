import unittest
import LoadPGToTempBQ as x

class TestLoadPGToTempBQ(unittest.TestCase):

    def test_get_contentID_keyName(self):
        self.assertEqual(x.get_contentID_keyName("pmr_pm_plan"), (36, "pm_id", "merge_pm_plan"))
        self.assertEqual(x.get_contentID_keyName("pmr_pm_item"), (37, "pm_item_id", "merge_pm_item"))


if __name__ == "__main__":
    unittest.main()
